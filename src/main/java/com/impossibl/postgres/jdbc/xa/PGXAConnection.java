/**
 * Copyright (c) 2014, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc.xa;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDirectConnection;
import com.impossibl.postgres.jdbc.PGPooledConnection;
import com.impossibl.postgres.protocol.TransactionStatus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * The PostgreSQL implementation of {@link XAConnection} and {@link XAResource}.
 *
 * This implementation doesn't support transaction interleaving
 * (see JTA specification, section 3.4.4) and suspend/resume.
 *
 * Two-phase commit requires PostgreSQL server version 8.1
 * or higher.
 *
 * @author Heikki Linnakangas (heikki.linnakangas@iki.fi)
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGXAConnection extends PGPooledConnection implements XAConnection, XAResource {
  /**
   * String constants
   */
  private static final String ERROR_ROLLING_BACK_PREPARED_TRANSACTION = "Error rolling back prepared transaction";
  private static final String TRANSACTION_INTERLEAVING_NOT_IMPLEMENTED = "Transaction interleaving not implemented";
  private static final String XID_MUST_NOT_BE_NULL = "xid must not be null";
  /**
   * Underlying physical database connection. It's used for issuing PREPARE TRANSACTION/
   * COMMIT PREPARED/ROLLBACK PREPARED commands.
   */
  private static final Logger logger = Logger.getLogger(PGXAConnection.class.getName());
  private final PGDirectConnection conn;

  /*
   * PGXAConnection-object can be in one of three states:
   *
   * IDLE
   * Not associated with a XA-transaction. You can still call
   * getConnection and use the connection outside XA. currentXid is null.
   * autoCommit is true on a connection by getConnection, per normal JDBC
   * rules, though the caller can change it to false and manage transactions
   * itself using Connection.commit and rollback.
   *
   * ACTIVE
   * start has been called, and we're associated with an XA transaction.
   * currentXid is valid. autoCommit is false on a connection returned by
   * getConnection, and should not be messed with by the caller or the XA
   * transaction will be broken.
   *
   * ENDED
   * end has been called, but the transaction has not yet been prepared.
   * currentXid is still valid. You shouldn't use the connection for anything
   * else than issuing a XAResource.commit or rollback.
   */
  private Xid currentXid;
  private String xidStr;
  private int state;

  static final int STATE_IDLE = 0;
  static final int STATE_ACTIVE = 1;
  static final int STATE_ENDED = 2;

  /*
   * When an XA transaction is started, we put the underlying connection
   * into non-autocommit mode. The old setting is saved in
   * localAutoCommitMode, so that we can restore it when the XA transaction
   * ends and the connection returns into local transaction mode.
   */
  private boolean localAutoCommitMode = true;

  private void debug(String s) {
    logger.log(Level.FINE, "XAResource " + Integer.toHexString(this.hashCode()) + ": " + s);
  }

  public PGXAConnection(PGDirectConnection conn) throws SQLException {
    super(conn, true, true);
    this.conn = conn;
    this.state = STATE_IDLE;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (logger.isLoggable(Level.FINE))
      debug("PGXAConnection.getConnection called");

    PGConnection c = (PGConnection)super.getConnection();

    // When we're outside an XA transaction, autocommit
    // is supposed to be true, per usual JDBC convention.
    // When an XA transaction is in progress, it should be
    // false.
    if (state == STATE_IDLE)
      conn.setAutoCommit(true);

    return new PGXAConnectionDelegator(this, c);
  }

  @Override
  public XAResource getXAResource() {
    return this;
  }

  /**** XAResource interface ****/

  /**
   * Preconditions:
   * 1. flags must be one of TMNOFLAGS, TMRESUME or TMJOIN
   * 2. xid != null
   * 3. connection must not be associated with a transaction
   * 4. the TM hasn't seen the xid before
   *
   * Implementation deficiency preconditions:
   * 1. TMRESUME not supported.
   * 2. if flags is TMJOIN, we must be in ended state,
   *    and xid must be the current transaction
   * 3. unless flags is TMJOIN, previous transaction using the
   *    connection must be committed or prepared or rolled back
   *
   * Postconditions:
   * 1. Connection is associated with the transaction
   */
  @Override
  public void start(Xid xid, int flags) throws XAException {
    if (logger.isLoggable(Level.FINE))
      debug("starting transaction xid = " + xid);

    // Check preconditions
    if (flags != XAResource.TMNOFLAGS && flags != XAResource.TMRESUME && flags != XAResource.TMJOIN)
      throw new PGXAException("Invalid flags", XAException.XAER_INVAL);

    if (xid == null)
      throw new PGXAException(XID_MUST_NOT_BE_NULL, XAException.XAER_INVAL);

    if (state == STATE_ACTIVE)
      throw new PGXAException("Connection is busy with another transaction", XAException.XAER_PROTO);

    // We can't check precondition 4 easily, so we don't. Duplicate xid will be catched in prepare phase.

    // Check implementation deficiency preconditions
    if (flags == TMRESUME)
      throw new PGXAException("suspend/resume not implemented", XAException.XAER_RMERR);

    // It's ok to join an ended transaction. WebLogic does that.
    if (flags == TMJOIN) {
      if (state != STATE_ENDED)
        throw new PGXAException(TRANSACTION_INTERLEAVING_NOT_IMPLEMENTED, XAException.XAER_RMERR);

      if (!xid.equals(currentXid))
        throw new PGXAException(TRANSACTION_INTERLEAVING_NOT_IMPLEMENTED, XAException.XAER_RMERR);
    }
    else if (state == STATE_ENDED)
      throw new PGXAException(TRANSACTION_INTERLEAVING_NOT_IMPLEMENTED, XAException.XAER_RMERR);

    if (flags == TMNOFLAGS) {
      try {
        localAutoCommitMode = conn.getAutoCommit();
        conn.setAutoCommit(false);
      }
      catch (SQLException ex) {
        throw new PGXAException("Error disabling autocommit", ex, XAException.XAER_RMERR);
      }
    }

    // Preconditions are met, Associate connection with the transaction
    state = STATE_ACTIVE;
    currentXid = xid;
    xidStr = null;
  }

  /**
   * Preconditions:
   * 1. Flags is one of TMSUCCESS, TMFAIL, TMSUSPEND
   * 2. xid != null
   * 3. Connection is associated with transaction xid
   *
   * Implementation deficiency preconditions:
   * 1. Flags is not TMSUSPEND
   *
   * Postconditions:
   * 1. connection is disassociated from the transaction.
   */
  @Override
  public void end(Xid xid, int flags) throws XAException {
    if (logger.isLoggable(Level.FINE))
      debug("ending transaction xid = " + xid);

    // Check preconditions

    if (flags != XAResource.TMSUSPEND && flags != XAResource.TMFAIL && flags != XAResource.TMSUCCESS)
      throw new PGXAException("Invalid flags", XAException.XAER_INVAL);

    if (xid == null)
      throw new PGXAException(XID_MUST_NOT_BE_NULL, XAException.XAER_INVAL);

    if (state != STATE_ACTIVE || !currentXid.equals(xid))
      throw new PGXAException("tried to call end without corresponding start call", XAException.XAER_PROTO);

    // Check implementation deficiency preconditions
    if (flags == XAResource.TMSUSPEND)
      throw new PGXAException("suspend/resume not implemented", XAException.XAER_RMERR);

    // We ignore TMFAIL. It's just a hint to the RM. We could roll back immediately
    // if TMFAIL was given.

    // All clear. We don't have any real work to do.
    state = STATE_ENDED;
  }

  /**
   * Preconditions:
   * 1. xid != null
   * 2. xid is in ended state
   *
   * Implementation deficiency preconditions:
   * 1. xid was associated with this connection
   *
   * Postconditions:
   * 1. Transaction is prepared
   */
  @Override
  public int prepare(Xid xid) throws XAException {
    if (logger.isLoggable(Level.FINE))
      debug("preparing transaction xid = " + xid);

    // Check preconditions
    if (currentXid == null)
      throw new PGXAException("Not associated with an xid", XAException.XAER_RMERR);

    if (!currentXid.equals(xid)) {
      throw new PGXAException("Not implemented: Prepare must be issued using the same connection that started the transaction",
                              XAException.XAER_RMERR);
    }
    if (state != STATE_ENDED)
      throw new PGXAException("Prepare called before end", XAException.XAER_INVAL);

    state = STATE_IDLE;
    currentXid = null;

    try {
      xidStr = RecoveredXid.xidToString(xid);

      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate("PREPARE TRANSACTION '" + xidStr + "'");
      }
      finally {
        stmt.close();
      }
      conn.setAutoCommit(localAutoCommitMode);

      return XA_OK;
    }
    catch (SQLTimeoutException ste) {
      throw new PGXAException("Error preparing transaction", ste, XAException.XAER_RMFAIL);
    }
    catch (SQLException ex) {
      throw new PGXAException("Error preparing transaction", ex, XAException.XAER_RMERR);
    }
  }

  /**
   * Preconditions:
   * 1. flag must be one of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS or TMSTARTTRSCAN | TMENDRSCAN
   * 2. if flag isn't TMSTARTRSCAN or TMSTARTRSCAN | TMENDRSCAN, a recovery scan must be in progress
   *
   * Postconditions:
   * 1. list of prepared xids is returned
   */
  @Override
  public Xid[] recover(int flag) throws XAException {
    // Check preconditions
    if (flag != TMSTARTRSCAN && flag != TMENDRSCAN && flag != TMNOFLAGS && flag != (TMSTARTRSCAN | TMENDRSCAN))
      throw new PGXAException("Invalid flag", XAException.XAER_INVAL);

    // We don't check for precondition 2, because we would have to add some additional state in
    // this object to keep track of recovery scans.

    // All clear. We return all the xids in the first TMSTARTRSCAN call, and always return
    // an empty array otherwise.
    if ((flag & TMSTARTRSCAN) == 0)
      return new Xid[0];
    else {
      // If this connection is simultaneously used for a transaction,
      // this query gets executed inside that transaction. It's OK,
      // except if the transaction is in abort-only state and the
      // backed refuses to process new queries. Hopefully not a problem
      // in practice.
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT gid FROM pg_prepared_xacts where database = current_database()")) {
        List<Xid> l = new ArrayList<>();
        while (rs.next()) {
          Xid recoveredXid = RecoveredXid.stringToXid(rs.getString(1));
          if (recoveredXid != null)
            l.add(recoveredXid);
        }

        return l.toArray(new Xid[l.size()]);
      }
      catch (SQLTimeoutException ste) {
        throw new PGXAException("Error during recover", ste, XAException.XAER_RMFAIL);
      }
      catch (SQLException ex) {
        throw new PGXAException("Error during recover", ex, XAException.XAER_RMERR);
      }
    }
  }

  /**
   * Preconditions:
   * 1. xid is known to the RM or it's in prepared state
   *
   * Implementation deficiency preconditions:
   * 1. xid must be associated with this connection if it's not in prepared state.
   *
   * Postconditions:
   * 1. Transaction is rolled back and disassociated from connection
   */
  @Override
  public void rollback(Xid xid) throws XAException {
    if (logger.isLoggable(Level.FINE))
      debug("rolling back xid = " + xid);

    if (xid == null)
      throw new PGXAException(XID_MUST_NOT_BE_NULL, XAException.XAER_INVAL);

    // We don't explicitly check precondition 1.

    try {
      if (currentXid != null && xid.equals(currentXid)) {
        state = STATE_IDLE;
        currentXid = null;
        xidStr = null;
        conn.rollback();
        conn.setAutoCommit(localAutoCommitMode);
      }
      else {
        String s = xidStr != null ? xidStr : RecoveredXid.xidToString(xid);

        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
          stmt.executeUpdate("ROLLBACK PREPARED '" + s + "'");
        }
        finally {
          xidStr = null;
          stmt.close();
        }
      }
    }
    catch (SQLTimeoutException ste) {
      throw new PGXAException(ERROR_ROLLING_BACK_PREPARED_TRANSACTION, ste, XAException.XAER_RMFAIL);
    }
    catch (SQLException ex) {
      if ("42704".equals(ex.getSQLState())) {
        throw new PGXAException(ERROR_ROLLING_BACK_PREPARED_TRANSACTION, ex, XAException.XAER_NOTA);
      }
      throw new PGXAException(ERROR_ROLLING_BACK_PREPARED_TRANSACTION, ex, XAException.XAER_RMERR);
    }
  }

  @Override
  public void commit(Xid xid, boolean onePhase) throws XAException {
    if (logger.isLoggable(Level.FINE))
      debug("committing xid = " + xid + (onePhase ? " (one phase) " : " (two phase)"));

    if (xid == null)
      throw new PGXAException(XID_MUST_NOT_BE_NULL, XAException.XAER_INVAL);

    if (onePhase)
      commitOnePhase(xid);
    else
      commitPrepared(xid);
  }

  /**
   * Preconditions:
   * 1. xid must in ended state.
   *
   * Implementation deficiency preconditions:
   * 1. this connection must have been used to run the transaction
   *
   * Postconditions:
   * 1. Transaction is committed
   */
  private void commitOnePhase(Xid xid) throws XAException {
    try {
      // Check preconditions
      if (currentXid == null || !currentXid.equals(xid)) {
        // In fact, we don't know if xid is bogus, or if it just wasn't associated with this connection.
        // Assume it's our fault.
        throw new PGXAException("Not implemented: one-phase commit must be issued using the same connection that was used to start it",
                                XAException.XAER_RMERR);
      }
      if (state != STATE_ENDED)
        throw new PGXAException("commit called before end", XAException.XAER_PROTO);

      // Preconditions are met. Commit
      state = STATE_IDLE;
      currentXid = null;
      xidStr = null;

      conn.commit();
      conn.setAutoCommit(localAutoCommitMode);
    }
    catch (SQLTimeoutException ste) {
      throw new PGXAException("Error during one-phase commit", ste, XAException.XAER_RMFAIL);
    }
    catch (SQLException ex) {
      throw new PGXAException("Error during one-phase commit", ex, XAException.XAER_RMERR);
    }
  }

  /**
   * Preconditions:
   * 1. xid must be in prepared state in the server
   *
   * Implementation deficiency preconditions:
   * 1. Connection must be in idle state
   *
   * Postconditions:
   * 1. Transaction is committed
   */
  private void commitPrepared(Xid xid) throws XAException {
    try {
      // Check preconditions. The connection mustn't be used for another
      // other XA or local transaction, or the COMMIT PREPARED command
      // would mess it up.
      if (state != STATE_IDLE || conn.getTransactionStatus() != TransactionStatus.Idle)
        throw new PGXAException("Not implemented: 2nd phase commit must be issued using an idle connection",
                                XAException.XAER_RMERR);

      String s = xidStr != null ? xidStr : RecoveredXid.xidToString(xid);

      localAutoCommitMode = conn.getAutoCommit();
      conn.setAutoCommit(true);
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate("COMMIT PREPARED '" + s + "'");
      }
      finally {
        xidStr = null;
        stmt.close();
        conn.setAutoCommit(localAutoCommitMode);
      }
    }
    catch (SQLTimeoutException ste) {
      throw new PGXAException("Error committing prepared transaction", ste, XAException.XAER_RMFAIL);
    }
    catch (SQLException ex) {
      if ("42704".equals(ex.getSQLState())) {
        throw new PGXAException("Error commiting prepared transaction", ex, XAException.XAER_NOTA);
      }
      throw new PGXAException("Error committing prepared transaction", ex, XAException.XAER_RMERR);
    }
  }

  @Override
  public boolean isSameRM(XAResource xares) throws XAException {
    // This trivial implementation makes sure that the
    // application server doesn't try to use another connection
    // for prepare, commit and rollback commands.
    return xares == this;
  }

  /**
   * Does nothing, since we don't do heuristics,
   */
  @Override
  public void forget(Xid xid) throws XAException {
    throw new PGXAException("Heuristic commit/rollback not supported", XAException.XAER_NOTA);
  }

  /**
   * We don't do transaction timeouts. Just returns 0.
   */
  @Override
  public int getTransactionTimeout() {
    return 0;
  }

  /**
   * We don't do transaction timeouts. Returns false.
   */
  @Override
  public boolean setTransactionTimeout(int seconds) {
    return false;
  }

  /**
   * Get the state
   * @return The value
   */
  int getState() {
    return state;
  }
}
