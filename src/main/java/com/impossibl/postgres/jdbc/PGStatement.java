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

import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.types.Type;
import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_STATEMENT;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.protocol.ServerObjectType.Statement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.util.concurrent.TimeUnit.SECONDS;

abstract class PGStatement implements Statement {



  PGConnection connection;
  int resultSetType;
  int resultSetConcurrency;
  int resultSetHoldability;
  int fetchDirection;
  String name;
  boolean processEscapes;
  List<ResultField> resultFields;
  Integer maxRows;
  Integer fetchSize;
  Integer maxFieldSize;
  QueryCommand command;
  List<QueryCommand.ResultBatch> resultBatches;
  boolean autoClose;
  List<PGResultSet> activeResultSets;
  PGResultSet generatedKeysResultSet;
  SQLWarning warningChain;
  int queryTimeout;



  PGStatement(PGConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, String name, List<ResultField> resultFields) {
    this.connection = connection;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    this.name = name;
    this.processEscapes = true;
    this.resultFields = resultFields;
    this.activeResultSets = new ArrayList<>();
  }

  protected void finalize() throws SQLException {
    close();
  }

  /**
   * Ensure the connection is not closed
   *
   * @throws SQLException
   *          If the connection is closed
   */
  void checkClosed() throws SQLException {

    if (isClosed())
      throw CLOSED_STATEMENT;
  }

  /**
   * Disposes of the named server object
   *
   * @param objectType
   *          Type of object to dispose of
   * @param objectName
   *          Name of the object to dispose of
   * @throws SQLException
   *          If an error occurs during disposal
   */
  void dispose(ServerObjectType objectType, String objectName) throws SQLException {

    if (objectName == null)
      return;

    CloseCommand close = connection.getProtocol().createClose(objectType, objectName);

    connection.execute(close, false);
  }

  void dispose(Command command) throws SQLException {

    if (command instanceof BindExecCommand) {

      dispose(ServerObjectType.Portal, ((BindExecCommand)command).getPortalName());
    }

  }

  /**
   * Closes all active result sets for this statement
   *
   * @throws SQLException
   *          If an error occurs closing a result set
   */
  void closeResultSets() throws SQLException {

    for (PGResultSet rs : activeResultSets) {
      rs.internalClose();
    }

    activeResultSets.clear();
    generatedKeysResultSet = null;

  }

  /**
   * Called by result sets to notify the statement of their closure. Removes
   * the result set from the active set of result sets. If auto-close is
   * enabled this closes the statement when the last result set is closed.
   *
   * @param resultSet
   *          The result set that is closing
   * @throws SQLException
   *          If an error occurs closing a result set
   */
  void handleResultSetClosure(PGResultSet resultSet) throws SQLException {

    activeResultSets.remove(resultSet);

    if (autoClose && activeResultSets.isEmpty()) {

      close();

    }

  }

  /**
   * Determines whether or not the current statement state requires a named
   * portal or could use the unnamed portal instead
   *
   * @return true when a named portal is required and false when it is not
   */
  boolean needsNamedPortal() {

    return fetchSize != null;
  }

  /**
   * Cleans up all resources, including active result sets
   *
   * @throws SQLException
   *          If an error occurs closing result sets or
   */
  void internalClose() throws SQLException {

    closeResultSets();

    dispose(Statement, name);

    connection = null;
    command = null;
    resultFields = null;
    resultBatches = null;
    generatedKeysResultSet = null;
  }

  boolean hasResults() {
    return !resultBatches.isEmpty() &&
        resultBatches.get(0).results != null;
  }

  boolean hasUpdateCount() {
    return !resultBatches.isEmpty() &&
        resultBatches.get(0).rowsAffected != null;
  }

  /**
   * Execute the named statement. It must have previously been parsed and
   * ready to be bound and executed.
   *
   * @param statementName Name of backend statement to execute or null
   * @param parameterTypes List of parameter types
   * @param parameterValues List of parmaeter values
   * @return true if command returned results or false if not
   * @throws SQLException
   *          If an error occurred durring statement execution
   */
  public boolean executeSimple(String sql) throws SQLException {

    closeResultSets();

    command = connection.getProtocol().createQuery(sql);

    if (maxFieldSize != null)
      command.setMaxFieldLength(maxFieldSize);

    warningChain = connection.execute(command, true);

    resultBatches = new ArrayList<>(command.getResultBatches());

    return hasResults();
  }

  /**
   * Execute the named statement. It must have previously been parsed and
   * ready to be bound and executed.
   *
   * @param statementName Name of backend statement to execute or null
   * @param parameterTypes List of parameter types
   * @param parameterValues List of parmaeter values
   * @return true if command returned results or false if not
   * @throws SQLException
   *          If an error occurred durring statement execution
   */
  public boolean executeStatement(String statementName, List<Type> parameterTypes, List<Object> parameterValues) throws SQLException {

    closeResultSets();

    String portalName = null;

    if (needsNamedPortal()) {
      portalName = connection.getNextPortalName();
    }

    BindExecCommand command = connection.getProtocol().createBindExec(portalName, statementName, parameterTypes, parameterValues, resultFields, Object[].class);

    //Set query timeout
    long queryTimeoutMS = SECONDS.toMillis(queryTimeout);
    command.setQueryTimeout(queryTimeoutMS);

    if (fetchSize != null)
      command.setMaxRows(fetchSize);

    if (maxFieldSize != null)
      command.setMaxFieldLength(maxFieldSize);

    this.warningChain = connection.execute(command, true);

    this.command = command;
    this.resultBatches = new ArrayList<>(command.getResultBatches());

    return hasResults();
  }

  PGResultSet createResultSet(List<ResultField> resultFields, List<Object[]> results) throws SQLException {
    return createResultSet(resultFields, results, connection.getTypeMap());
  }

  PGResultSet createResultSet(List<ResultField> resultFields, List<Object[]> results, Map<String, Class<?>> typeMap) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, resultFields, results);
    activeResultSets.add(resultSet);
    return resultSet;
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkClosed();

    return connection;
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkClosed();

    return resultSetType;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    checkClosed();

    return resultSetConcurrency;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    checkClosed();

    return resultSetHoldability;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    checkClosed();
    // TODO implement
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    checkClosed();

    return autoClose;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    checkClosed();

    autoClose = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    checkClosed();

    return maxFieldSize != null ? maxFieldSize : 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();

    if (max < 0)
      throw ILLEGAL_ARGUMENT;

    maxFieldSize = max;
  }

  @Override
  public int getMaxRows() throws SQLException {
    checkClosed();
    return maxRows != null ? maxRows : 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    checkClosed();

    if (max < 0)
      throw ILLEGAL_ARGUMENT;

    maxRows = max;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();

    return fetchDirection;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();

    if (direction != ResultSet.FETCH_FORWARD &&
        direction != ResultSet.FETCH_REVERSE &&
        direction != ResultSet.FETCH_UNKNOWN)
      throw ILLEGAL_ARGUMENT;

    fetchDirection = direction;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClosed();

    return fetchSize != null ? fetchSize : 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClosed();

    if (rows < 0)
      throw ILLEGAL_ARGUMENT;

    fetchSize = rows;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    checkClosed();

    this.processEscapes = enable;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    checkClosed();
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int queryTimeout) throws SQLException {
    checkClosed();

    if (queryTimeout < 0) {
      throw new SQLException("invalid query timeout");
    }

    this.queryTimeout = queryTimeout;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public PGResultSet getResultSet() throws SQLException {
    checkClosed();

    if (generatedKeysResultSet != null ||
        command == null ||
        !hasResults()) {
      return null;
    }

    QueryCommand.ResultBatch resultBatch = resultBatches.get(0);

    PGResultSet rs = new PGResultSet(this, ResultSet.CONCUR_READ_ONLY, command, resultBatch.fields, resultBatch.results);

    this.activeResultSets.add(rs);

    return rs;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkClosed();

    if (command == null || !hasUpdateCount()) {
      return -1;
    }

    return (int) (long) resultBatches.get(0).rowsAffected;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return getMoreResults(CLOSE_ALL_RESULTS);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    checkClosed();

    if (resultBatches.isEmpty()) {
      return false;
    }

    resultBatches.remove(0);

    return hasResults();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();

    if (generatedKeysResultSet == null) {
      return createResultSet(Collections.<ResultField>emptyList(), Collections.<Object[]>emptyList());
    }

    return generatedKeysResultSet;
  }

  @Override
  public void cancel() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return connection == null;
  }

  @Override
  public void close() throws SQLException {

    // Ignore multiple closes
    if (isClosed())
      return;

    connection.handleStatementClosure(this);

    internalClose();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkClosed();

    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkClosed();

    warningChain = null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (!iface.isAssignableFrom(getClass())) {
      throw UNWRAP_ERROR;
    }

    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

}
