/**
 * Copyright (c) 2013, impossibl.com
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
package com.impossibl.postgres.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

/**
 * PooledConnection implementation
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGPooledConnection implements PooledConnection {
  private List<ConnectionEventListener> connectionListeners;
  private List<StatementEventListener> statementListeners;
  private PGConnectionImpl con;
  private PGPooledConnectionDelegator last;
  private final boolean autoCommit;
  private final boolean isXA;

  /**
   * Creates a new PooledConnection representing the specified physical
   * connection.
   */
  public PGPooledConnection(PGConnectionImpl con, boolean autoCommit, boolean isXA) {
    this.connectionListeners = new ArrayList<ConnectionEventListener>();
    this.statementListeners = new ArrayList<StatementEventListener>();
    this.con = con;
    this.last = null;
    this.autoCommit = autoCommit;
    this.isXA = isXA;
  }

  /**
   * Set last
   * @param v The value
   */
  void setLast(PGPooledConnectionDelegator v) {
    last = v;
  }

  /**
   * Is XA
   * @return The value
   */
  boolean isXA() {
    return isXA;
  }

  /**
   * {@inheritDoc}
   */
  public void addConnectionEventListener(ConnectionEventListener connectionEventListener) {
    connectionListeners.add(connectionEventListener);
  }

  /**
   * {@inheritDoc}
   */
  public void removeConnectionEventListener(ConnectionEventListener connectionEventListener) {
    connectionListeners.remove(connectionEventListener);
  }

  /**
   * {@inheritDoc}
   */
  public void addStatementEventListener(StatementEventListener statementEventListener) {
    statementListeners.add(statementEventListener);
  }

  /**
   * {@inheritDoc}
   */
  public void removeStatementEventListener(StatementEventListener statementEventListener) {
    statementListeners.remove(statementEventListener);
  }

  /**
   * Closes the physical database connection represented by this
   * PooledConnection.  If any client has a connection based on
   * this PooledConnection, it is forcibly closed as well.
   */
  public void close() throws SQLException {
    if (last != null) {
      last.reset();
      if (!con.getAutoCommit()) {
        try {
          con.rollback();
        }
        catch (SQLException e) {
          // Ignore
        }
      }
    }

    try {
      con.close();
    }
    finally {
      con = null;
    }
  }

  /**
   * Gets a handle for a client to use.  This is a wrapper around the
   * physical connection, so the client can call close and it will just
   * return the connection to the pool without really closing the
   * pgysical connection.
   *
   * <p>According to the JDBC 2.0 Optional Package spec (6.2.3), only one
   * client may have an active handle to the connection at a time, so if
   * there is a previous handle active when this is called, the previous
   * one is forcibly closed and its work rolled back.</p>
   */
  public Connection getConnection() throws SQLException {
    if (con == null) {
      // Before throwing the exception, let's notify the registered listeners about the error
      PGSQLSimpleException sqlException = new PGSQLSimpleException("This PooledConnection has already been closed.",
                                                                   "08003");
      fireConnectionFatalError(sqlException);
      throw sqlException;
    }
    // If any error occures while opening a new connection, the listeners
    // have to be notified. This gives a chance to connection pools to
    // eliminate bad pooled connections.
    try {
      // Only one connection can be open at a time from this PooledConnection.  See JDBC 2.0 Optional Package spec section 6.2.3
      if (last != null) {
        last.reset();
        if (!con.getAutoCommit()) {
          try {
            con.rollback();
          }
          catch (SQLException e) {
            // Ignore
          }
        }
        con.clearWarnings();
      }

      /*
       * In XA-mode, autocommit is handled in PGXAConnection,
       * because it depends on whether an XA-transaction is open
       * or not
       */
      if (!isXA)
        con.setAutoCommit(autoCommit);
    }
    catch (SQLException sqlException) {
      fireConnectionFatalError(sqlException);
      throw (SQLException)sqlException.fillInStackTrace();
    }

    PGPooledConnectionDelegator handler = new PGPooledConnectionDelegator(this, con);

    last = handler;

    return handler;
  }

  /**
   * Used to fire a connection closed event to all listeners.
   */
  void fireConnectionClosed() {
    ConnectionEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    ConnectionEventListener[] local = (ConnectionEventListener[]) connectionListeners.toArray(new ConnectionEventListener[connectionListeners.size()]);
    for (int i = 0; i < local.length; i++) {
      ConnectionEventListener listener = local[i];
      if (evt == null) {
        evt = createConnectionEvent(null);
      }
      listener.connectionClosed(evt);
    }
  }

  /**
   * Used to fire a connection error event to all listeners.
   */
  void fireConnectionFatalError(SQLException e) {
    ConnectionEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    ConnectionEventListener[] local = (ConnectionEventListener[])connectionListeners.toArray(new ConnectionEventListener[connectionListeners.size()]);
    for (int i = 0; i < local.length; i++) {
      ConnectionEventListener listener = local[i];
      if (evt == null) {
        evt = createConnectionEvent(e);
      }
      listener.connectionErrorOccurred(evt);
    }
  }

  protected ConnectionEvent createConnectionEvent(SQLException sqle) {
    return new ConnectionEvent(this, sqle);
  }

  // Classes we consider fatal.
  private static String[] fatalClasses = {
    "08",  // connection error
    "53",  // insufficient resources

    // nb: not just "57" as that includes query cancel which is nonfatal
    "57P01",  // admin shutdown
    "57P02",  // crash shutdown
    "57P03",  // cannot connect now

    "58",  // system error (backend)
    "60",  // system error (driver)
    "99",  // unexpected error
    "F0",  // configuration file error (backend)
    "XX",  // internal error (backend)
  };

  private static boolean isFatalState(String state) {
    if (state == null)      // no info, assume fatal
      return true;
    if (state.length() < 2) // no class info, assume fatal
      return true;

    for (int i = 0; i < fatalClasses.length; ++i)
      if (state.startsWith(fatalClasses[i]))
        return true; // fatal

    return false;
  }

  /**
   * Fires a connection error event, but only if we
   * think the exception is fatal.
   *
   * @param e the SQLException to consider
   */
  public void fireConnectionError(SQLException e) {
    if (!isFatalState(e.getSQLState()))
      return;

    fireConnectionFatalError(e);
  }

  protected StatementEvent createStatementEvent(PreparedStatement ps, SQLException sqle) {
    if (sqle == null)
      return new StatementEvent(this, ps);
    else
      return new StatementEvent(this, ps, sqle);
  }

  /**
   * Used to fire a statement closed event to all listeners.
   */
  void fireStatementClosed(PreparedStatement ps) {
    StatementEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    StatementEventListener[] local = (StatementEventListener[]) statementListeners.toArray(new StatementEventListener[statementListeners.size()]);
    for (int i = 0; i < local.length; i++) {
      StatementEventListener listener = local[i];
      if (evt == null) {
        evt = createStatementEvent(ps, null);
      }
      listener.statementClosed(evt);
    }
  }

  /**
   * Used to fire a statement error event to all listeners.
   */
  void fireStatementError(PreparedStatement ps, SQLException se) {
    StatementEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    StatementEventListener[] local = (StatementEventListener[]) statementListeners.toArray(new StatementEventListener[statementListeners.size()]);
    for (int i = 0; i < local.length; i++) {
      StatementEventListener listener = local[i];
      if (evt == null) {
        evt = createStatementEvent(ps, se);
      }
      listener.statementErrorOccurred(evt);
    }
  }
}
