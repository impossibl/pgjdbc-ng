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

import com.impossibl.postgres.jdbc.Housekeeper.CleanupRunnable;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.DataRow;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_STATEMENT;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.protocol.QueryCommand.ResultBatch.releaseResultBatches;
import static com.impossibl.postgres.protocol.ServerObjectType.Statement;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

abstract class PGStatement implements Statement {

  protected static final String CACHED_STATEMENT_PREFIX = "cached-";
  protected static final String NO_CACHE_STATEMENT_PREFIX = "nocache-";

  /**
   * Cleans up server resources in the event of leaking statements
   *
   * @author kdubb
   *
   */
  static class Cleanup implements CleanupRunnable {

    PGConnectionImpl connection;
    String name;
    List<WeakReference<PGResultSet>> resultSets;
    StackTraceElement[] allocationStackTrace;

    public Cleanup(PGConnectionImpl connection, String name, List<WeakReference<PGResultSet>> resultSets) {
      this.connection = connection;
      this.name = name;
      this.resultSets = resultSets;
      this.allocationStackTrace = new Exception().getStackTrace();
    }

    @Override
    public String getKind() {
      return "statement";
    }

    @Override
    public StackTraceElement[] getAllocationStackTrace() {
      return allocationStackTrace;
    }

    @Override
    public void run() {

      closeResultSets(resultSets);

      try {
        dispose(connection, ServerObjectType.Statement, name);
      }
      catch (SQLException e) {
        // Ignore...
      }

      connection.handleStatementClosure(null);
      connection = null;
    }

  }



  PGConnectionImpl connection;
  String cursorName;
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
  List<WeakReference<PGResultSet>> activeResultSets;
  PGResultSet generatedKeysResultSet;
  SQLWarning warningChain;
  int queryTimeout;
  final Housekeeper.Ref housekeeper;
  final Object cleanupKey;


  PGStatement(PGConnectionImpl connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, String name, List<ResultField> resultFields) {
    super();

    this.connection = connection;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    this.fetchDirection = ResultSet.FETCH_FORWARD;
    this.name = name;
    this.processEscapes = true;
    this.resultFields = resultFields;
    this.activeResultSets = new ArrayList<>();
    this.generatedKeysResultSet = null;
    this.fetchSize = connection.getDefaultFetchSize() != Settings.DEFAULT_FETCH_SIZE_DEFAULT ? Integer.valueOf(connection.getDefaultFetchSize()) : null;

    this.housekeeper = connection.housekeeper;

    if (this.housekeeper != null)
      this.cleanupKey = this.housekeeper.add(this, new Cleanup(connection, name, activeResultSets));
    else
      this.cleanupKey = null;
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
  public static void dispose(PGConnectionImpl connection, ServerObjectType objectType, String objectName) throws SQLException {

    if (objectName == null)
      return;

    CloseCommand close = connection.getProtocol().createClose(objectType, objectName);

    connection.execute(close, false);
  }

  void dispose(Command command) throws SQLException {

    if (command instanceof BindExecCommand) {

      dispose(connection, ServerObjectType.Portal, ((BindExecCommand)command).getPortalName());
    }

  }

  /**
   * Closes the given list of result-sets
   *
   * @throws SQLException
   *          If an error occurs closing a result set
   */
  static void closeResultSets(List<WeakReference<PGResultSet>> resultSets) {

    for (WeakReference<PGResultSet> resultSetRef : resultSets) {

      PGResultSet resultSet = resultSetRef.get();
      if (resultSet != null) {
        try {
          resultSet.internalClose();
        }
        catch (SQLException e) {
          //Ignore...
        }
      }

    }

    resultSets.clear();

  }

  /**
   * Closes all active result sets for this statement
   *
   * @throws SQLException
   *          If an error occurs closing a result set
   */
  void closeResultSets() throws SQLException {
    closeResultSets(activeResultSets);

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

    //Remove given & abandoned result sets
    Iterator<WeakReference<PGResultSet>> resultSetRefIter = activeResultSets.iterator();
    while (resultSetRefIter.hasNext()) {

      WeakReference<PGResultSet> resultSetRef = resultSetRefIter.next();
      PGResultSet rs = resultSetRef.get();
      if (rs == null) {
        resultSetRefIter.remove();
      }
      else if (rs == resultSet) {
        resultSetRefIter.remove();
        break;
      }
    }

    //Handle auto closing
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
    resultBatches = releaseResultBatches(resultBatches);

    if (name != null && !name.startsWith(CACHED_STATEMENT_PREFIX)) {
      dispose(connection, Statement, name);
    }

    if (housekeeper != null)
      housekeeper.remove(cleanupKey);

    connection = null;
    command = null;
    resultFields = null;
    generatedKeysResultSet = null;
  }

  boolean hasResults() {
    return !resultBatches.isEmpty() &&
      resultBatches.get(0).getResults() != null;
  }

  boolean hasUpdateCount() {
    return !resultBatches.isEmpty() &&
      resultBatches.get(0).getRowsAffected() != null;
  }

  /**
   * Execute the given sql
   *
   * @param sql
   *          SQL text to execute
   * @return True if command returned results or false if not
   * @throws SQLException
   *           If an error occurred during statement execution
   */
  public boolean executeSimple(String sql) throws SQLException {

    closeResultSets();
    resultBatches = releaseResultBatches(resultBatches);

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
    resultBatches = releaseResultBatches(resultBatches);

    String portalName = null;

    if (needsNamedPortal()) {
      portalName = connection.getNextPortalName();
    }

    BindExecCommand command = connection.getProtocol().createBindExec(portalName, statementName, parameterTypes, parameterValues, resultFields);

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

  PGResultSet createResultSet(List<ResultField> resultFields, List<DataRow> results, boolean releaseResults) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, resultFields, results, releaseResults);
    activeResultSets.add(new WeakReference<>(resultSet));
    return resultSet;
  }

  PGResultSet createResultSet(QueryCommand command, List<ResultField> resultFields, List<DataRow> results) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, command, resultFields, results);
    activeResultSets.add(new WeakReference<>(resultSet));
    return resultSet;
  }

  PGResultSet createResultSet(String cursorName, int resultSetType, int resultSetHoldability, List<ResultField> resultFields) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, cursorName, resultSetType, resultSetHoldability, resultFields);
    activeResultSets.add(new WeakReference<>(resultSet));
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

  String getCursorName() {

    if (cursorName == null) {
      return "cursor" + super.hashCode();
    }

    return cursorName;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    checkClosed();

    this.cursorName = name;
  }

  @Override
  public PGResultSet getResultSet() throws SQLException {
    checkClosed();

    if (generatedKeysResultSet != null ||
        command == null ||
        !hasResults()) {
      return null;
    }

    if (cursorName != null) {

      QueryCommand.ResultBatch resultBatch = resultBatches.get(0);

      //Shouldn't be any actual rows, but release it to any buffers
      resultBatch.release();

      return createResultSet(getCursorName(), resultSetType, resultSetHoldability, resultBatch.getFields());
    }
    else if (command.getStatus() == QueryCommand.Status.Completed) {

      QueryCommand.ResultBatch resultBatch = resultBatches.get(0);

      // This batch can be re-used so let "close" or "getMoreResults" release it

      return createResultSet(resultBatch.getFields(), resultBatch.getResults(), false);
    }
    else {
      QueryCommand.ResultBatch resultBatch = resultBatches.remove(0);

      // The batch cannot be re-used, but the result set will clean it up when it's
      // done using it

      PGResultSet rs = createResultSet(command, resultBatch.getFields(), resultBatch.getResults());

      // Command cannot be re-used when portal'd batching is in progress
      command = null;

      return rs;
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkClosed();

    if (command == null || !hasUpdateCount()) {
      return -1;
    }

    return (int) (long) resultBatches.get(0).getRowsAffected();
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

    QueryCommand.ResultBatch finishedBatch = resultBatches.remove(0);
    finishedBatch.release();

    return hasResults();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();

    if (generatedKeysResultSet == null) {
      return createResultSet(Collections.<ResultField>emptyList(), Collections.<DataRow>emptyList(), false);
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

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLargeUpdateCount() throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLargeMaxRows() throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    throw NOT_IMPLEMENTED;
  }
}
