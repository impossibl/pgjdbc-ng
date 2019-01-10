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
package com.impossibl.postgres.jdbc.xa;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGPooledConnectionDelegator;
import com.impossibl.postgres.jdbc.PGSQLSimpleException;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * Connection handle for PGXAConnection
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGXAConnectionDelegator extends PGPooledConnectionDelegator {
  private PGXAConnection owner;

  /**
   * Constructor
   * @param owner The owner
   * @param delegator The delegator
   */
  public PGXAConnectionDelegator(PGXAConnection owner, PGConnection delegator) {
    super(owner, delegator);
    this.owner = owner;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commit() throws SQLException {
    if (owner.getState() != PGXAConnection.STATE_IDLE) {
      SQLException se = new PGSQLSimpleException("commit not allowed", "55000");
      owner.fireConnectionError(se);
      throw se;
    }
    super.commit();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() throws SQLException {
    if (owner.getState() != PGXAConnection.STATE_IDLE) {
      SQLException se = new PGSQLSimpleException("rollback not allowed", "55000");
      owner.fireConnectionError(se);
      throw se;
    }
    super.rollback();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    if (owner.getState() != PGXAConnection.STATE_IDLE) {
      SQLException se = new PGSQLSimpleException("rollback(Savepoint) not allowed", "55000");
      owner.fireConnectionError(se);
      throw se;
    }
    super.rollback(savepoint);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (owner.getState() != PGXAConnection.STATE_IDLE && autoCommit) {
      SQLException se = new PGSQLSimpleException("setAutoCommit(true) not allowed", "55000");
      owner.fireConnectionError(se);
      throw se;
    }
    super.setAutoCommit(autoCommit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof PGXAConnectionDelegator))
      return false;

    PGXAConnectionDelegator other = (PGXAConnectionDelegator)o;
    return owner.equals(other.owner);
  }
}
