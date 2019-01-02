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

import com.impossibl.postgres.api.jdbc.PGAnyType;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Connection handle for PGPooledConnection
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGPooledConnectionDelegator implements PGConnection {
  private PGPooledConnection owner;
  private PGConnection delegator;
  private boolean automatic;

  /**
   * Constructor
   * @param owner The owner
   * @param delegator The delegator
   */
  public PGPooledConnectionDelegator(PGPooledConnection owner, PGConnection delegator) {
    this.owner = owner;
    this.delegator = delegator;
    this.automatic = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isServerMinimumVersion(int major, int minor) {
    try {
      checkClosed();
      return delegator.isServerMinimumVersion(major, minor);
    }
    catch (SQLException se) {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addNotificationListener(String name, String channelNameFilter, PGNotificationListener listener) {
    try {
      checkClosed();
      delegator.addNotificationListener(name, channelNameFilter, listener);
    }
    catch (SQLException se) {
      // Nothing to do
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addNotificationListener(String channelNameFilter, PGNotificationListener listener) {
    try {
      checkClosed();
      delegator.addNotificationListener(channelNameFilter, listener);
    }
    catch (SQLException se) {
      // Nothing to do
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addNotificationListener(PGNotificationListener listener) {
    try {
      checkClosed();
      delegator.addNotificationListener(listener);
    }
    catch (SQLException se) {
      // Nothing to do
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeNotificationListener(String name) {
    try {
      checkClosed();
      delegator.removeNotificationListener(name);
    }
    catch (SQLException se) {
      // Nothing to do
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeNotificationListener(PGNotificationListener listener) {
    try {
      checkClosed();
      delegator.removeNotificationListener(listener);
    }
    catch (SQLException se) {
      // Nothing to do
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void abort(Executor executor) throws SQLException {
    try {
      checkClosed();
      delegator.abort(executor);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearWarnings() throws SQLException {
    try {
      checkClosed();
      delegator.clearWarnings();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
    if (delegator != null) {
      SQLException ex = null;
      if (!owner.isXA() && !delegator.getAutoCommit()) {
        try {
          delegator.rollback();
        }
        catch (SQLException e) {
          ex = e;
        }
      }
      delegator.clearWarnings();
      delegator = null;

      owner.setLast(null);
      owner.fireConnectionClosed();
      if (ex != null) {
        throw ex;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commit() throws SQLException {
    try {
      checkClosed();
      delegator.commit();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    try {
      checkClosed();
      return delegator.createArrayOf(typeName, elements);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Blob createBlob() throws SQLException {
    try {
      checkClosed();
      return delegator.createBlob();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Clob createClob() throws SQLException {
    try {
      checkClosed();
      return delegator.createClob();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NClob createNClob() throws SQLException {
    try {
      checkClosed();
      return delegator.createNClob();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SQLXML createSQLXML() throws SQLException {
    try {
      checkClosed();
      return delegator.createSQLXML();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement() throws SQLException {
    try {
      checkClosed();
      return new PGStatementDelegator(this, delegator.createStatement());
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    try {
      checkClosed();
      return new PGStatementDelegator(this, delegator.createStatement(resultSetType, resultSetConcurrency));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    try {
      checkClosed();
      return new PGStatementDelegator(this, delegator.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    try {
      checkClosed();
      return delegator.createStruct(typeName, attributes);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getAutoCommit() throws SQLException {
    try {
      checkClosed();
      return delegator.getAutoCommit();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCatalog() throws SQLException {
    try {
      checkClosed();
      return delegator.getCatalog();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getClientInfo() throws SQLException {
    try {
      checkClosed();
      return delegator.getClientInfo();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    try {
      checkClosed();
      return delegator.getClientInfo(name);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getHoldability() throws SQLException {
    try {
      checkClosed();
      return delegator.getHoldability();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    try {
      checkClosed();
      return delegator.getMetaData();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getNetworkTimeout() throws SQLException {
    try {
      checkClosed();
      return delegator.getNetworkTimeout();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchema() throws SQLException {
    try {
      checkClosed();
      return delegator.getSchema();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getTransactionIsolation() throws SQLException {
    try {
      checkClosed();
      return delegator.getTransactionIsolation();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    try {
      checkClosed();
      return delegator.getTypeMap();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    try {
      checkClosed();
      return delegator.getWarnings();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isClosed() throws SQLException {
    return delegator == null ? Boolean.TRUE : Boolean.FALSE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    try {
      checkClosed();
      return delegator.isReadOnly();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isValid(int timeout) throws SQLException {
    try {
      checkClosed();
      return delegator.isValid(timeout);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String nativeSQL(String sql) throws SQLException {
    try {
      checkClosed();
      return delegator.nativeSQL(sql);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    try {
      checkClosed();
      return new PGCallableStatementDelegator(this, delegator.prepareCall(sql));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    try {
      checkClosed();
      return new PGCallableStatementDelegator(this, delegator.prepareCall(sql, resultSetType, resultSetConcurrency));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    try {
      checkClosed();
      return new PGCallableStatementDelegator(this, delegator.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql, autoGeneratedKeys));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql, columnIndexes));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    try {
      checkClosed();
      return new PGPreparedStatementDelegator(this, delegator.prepareStatement(sql, columnNames));
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    try {
      checkClosed();
      delegator.releaseSavepoint(savepoint);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() throws SQLException {
    try {
      checkClosed();
      delegator.rollback();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    try {
      checkClosed();
      delegator.rollback(savepoint);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    try {
      checkClosed();
      delegator.setAutoCommit(autoCommit);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCatalog(String catalog) throws SQLException {
    try {
      checkClosed();
      delegator.setCatalog(catalog);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      checkClosed();
      delegator.setClientInfo(properties);
    }
    catch (SQLClientInfoException scie) {
      owner.fireConnectionError(scie);
      throw scie;
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);

      SQLClientInfoException scie = new SQLClientInfoException();
      scie.initCause(se);
      throw scie;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      checkClosed();
      delegator.setClientInfo(name, value);
    }
    catch (SQLClientInfoException scie) {
      owner.fireConnectionError(scie);
      throw scie;
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);

      SQLClientInfoException scie = new SQLClientInfoException();
      scie.initCause(se);
      throw scie;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setHoldability(int holdability) throws SQLException {
    try {
      delegator.setHoldability(holdability);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    try {
      checkClosed();
      delegator.setNetworkTimeout(executor, milliseconds);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    try {
      checkClosed();
      delegator.setReadOnly(readOnly);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    try {
      checkClosed();
      return delegator.setSavepoint();
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    try {
      checkClosed();
      return delegator.setSavepoint(name);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSchema(String schema) throws SQLException {
    try {
      checkClosed();
      delegator.setSchema(schema);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    try {
      checkClosed();
      delegator.setTransactionIsolation(level);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    try {
      checkClosed();
      delegator.setTypeMap(map);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    try {
      checkClosed();
      return delegator.isWrapperFor(iface);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      checkClosed();
      return delegator.unwrap(iface);
    }
    catch (SQLException se) {
      owner.fireConnectionError(se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setStrictMode(boolean v) {
    delegator.setStrictMode(v);
  }

  /**
   * {@inheritDoc}
   */
  public boolean isStrictMode() {
    return delegator.isStrictMode();
  }

  /**
   * {@inheritDoc}
   */
  public void setDefaultFetchSize(int v) {
    delegator.setDefaultFetchSize(v);
  }

  /**
   * {@inheritDoc}
   */
  public int getDefaultFetchSize() {
    return delegator.getDefaultFetchSize();
  }

  /**
   * {@inheritDoc}
   */
  public PGAnyType resolveType(String name) throws SQLException {
    return delegator.resolveType(name);
  }

  void reset() {
    if (delegator != null) {
      automatic = true;
    }
    delegator = null;
  }

  void fireConnectionError(SQLException se) {
    owner.fireConnectionError(se);
  }

  void fireStatementClosed(PreparedStatement ps) {
    owner.fireStatementClosed(ps);
  }

  void fireStatementError(PreparedStatement ps, SQLException se) {
    owner.fireStatementError(ps, se);
  }

  private void checkClosed() throws SQLException {
    if (delegator == null) {
      throw new PGSQLSimpleException(automatic ? "Connection has been closed automatically" : "Connection has been closed",
                                     "08003");
    }
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
    if (o == null || !(o instanceof PGPooledConnectionDelegator))
      return false;

    PGPooledConnectionDelegator other = (PGPooledConnectionDelegator)o;
    return delegator.equals(other.delegator);
  }
}
