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
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultBatches;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.system.Settings;

import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_STATEMENT;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;


abstract class PGStatement implements Statement {

  static final String CACHED_STATEMENT_PREFIX = "cached-";
  static final String NO_CACHE_STATEMENT_PREFIX = "nocache-";

  /**
   * Cleans up server resources in the event of leaking statements
   *
   * @author kdubb
   *
   */
  private static class Cleanup implements CleanupRunnable {

    PGDirectConnection connection;
    String name;
    List<WeakReference<PGResultSet>> resultSets;
    StackTraceElement[] allocationStackTrace;

    private Cleanup(PGDirectConnection connection, String name, List<WeakReference<PGResultSet>> resultSets) {
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



  PGDirectConnection connection;
  String cursorName;
  int resultSetType;
  int resultSetConcurrency;
  int resultSetHoldability;
  int fetchDirection;
  String name;
  boolean processEscapes;
  ResultField[] resultFields;
  Integer maxRows;
  Integer fetchSize;
  Integer maxFieldSize;
  Query query;
  List<ResultBatch> resultBatches;
  boolean autoClose;
  List<WeakReference<PGResultSet>> activeResultSets;
  PGResultSet generatedKeysResultSet;
  SQLWarning warningChain;
  int queryTimeout;
  final Housekeeper.Ref housekeeper;
  final Object cleanupKey;


  PGStatement(PGDirectConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, String name, ResultField[] resultFields) {
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
    this.fetchSize = connection.getDefaultFetchSize() != Settings.DEFAULT_FETCH_SIZE_DEFAULT ? connection.getDefaultFetchSize() : null;

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
  @SuppressWarnings("ThrowableNotThrown")
  static void dispose(PGDirectConnection connection, ServerObjectType objectType, String objectName) throws SQLException {

    if (objectName == null)
      return;

    connection.execute((long timeout) -> connection.getRequestExecutor().close(objectType, objectName));
  }

  /**
   * Closes the given list of result-sets
   *
   */
  private static void closeResultSets(List<WeakReference<PGResultSet>> resultSets) {

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
   */
  void closeResultSets() {
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
   * Cleans up all resources, including active result sets
   *
   * @throws SQLException
   *          If an error occurs closing result sets or
   */
  void internalClose() throws SQLException {

    closeResultSets();
    resultBatches = ResultBatches.releaseAll(resultBatches);

    if (name != null && !name.startsWith(CACHED_STATEMENT_PREFIX)) {
      dispose(connection, Statement, name);
    }

    if (housekeeper != null)
      housekeeper.remove(cleanupKey);

    connection = null;
    query = null;
    resultFields = null;
    generatedKeysResultSet = null;
  }

  private boolean hasResults() {
    return !resultBatches.isEmpty() &&
      !resultBatches.get(0).getResults().isEmpty();
  }

  private boolean hasUpdateCount() {
    return !resultBatches.isEmpty() &&
      resultBatches.get(0).getRowsAffected() != null;
  }

  /**
   * Execute the sql text using extended query cycle.
   *
   * @param sqlText SQL text to execute
   * @return true if command returned results or false if not
   * @throws SQLException
   *          If an error occurred during statement execution
   */
  boolean executeDirect(String sqlText) throws SQLException {

    try {

      closeResultSets();
      resultBatches = ResultBatches.releaseAll(resultBatches);

      Query query = Query.create(sqlText);

      query.setTimeout(SECONDS.toMillis(queryTimeout));
      query.setMaxRows(fetchSize);

      this.warningChain = query.execute(connection);

      this.query = query;
      this.resultBatches = query.getResultBatches();

      return hasResults();
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Throwable e) {
      throw new SQLException(e);
    }

  }

  /**
   * Execute the named statement. It must have previously been parsed and
   * ready to be bound and executed.
   *
   * @param statementName Name of backend statement to execute or null
   * @param parameterFormats List of parameter formats
   * @param parameterValues List of parameter values
   * @return true if command returned results or false if not
   * @throws SQLException
   *          If an error occurred during statement execution
   */
  boolean executeStatement(String statementName, FieldFormat[] parameterFormats, ByteBuf[] parameterValues) throws SQLException {

    try {

      closeResultSets();
      resultBatches = ResultBatches.releaseAll(resultBatches);

      Query query = Query.create(statementName, parameterFormats, parameterValues, resultFields);

      query.setTimeout(SECONDS.toMillis(queryTimeout));
      query.setMaxRows(fetchSize);

      this.warningChain = query.execute(connection);

      this.query = query;
      this.resultBatches = query.getResultBatches();

      return hasResults();
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Throwable e) {
      throw new SQLException(e);
    }

  }

  PGResultSet createResultSet(ResultField[] resultFields, List<RowData> results, boolean releaseResults, Map<String, Class<?>> typeMap) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, resultFields, results, releaseResults, typeMap);
    activeResultSets.add(new WeakReference<>(resultSet));
    return resultSet;
  }

  private PGResultSet createResultSet(Query query, ResultField[] resultFields, List<RowData> results) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, query, resultFields, results);
    activeResultSets.add(new WeakReference<>(resultSet));
    return resultSet;
  }

  private PGResultSet createResultSet(String cursorName, int resultSetType, int resultSetHoldability, ResultField[] resultFields) throws SQLException {

    PGResultSet resultSet = new PGResultSet(this, cursorName, resultSetType, resultSetHoldability, resultFields);
    activeResultSets.add(new WeakReference<>(resultSet));
    return resultSet;
  }

  @Override
  public PGDirectConnection getConnection() throws SQLException {
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
    else if (max == 0)
      maxFieldSize = null;
    else
      maxFieldSize = max;

    for (WeakReference<PGResultSet> resultSetRef : activeResultSets) {
      PGResultSet resultSet = resultSetRef.get();
      if (resultSet != null) {
        resultSet.updateMaxFieldSize(maxFieldSize);
      }
    }
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

  private String getCursorName() {

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
        query == null ||
        !hasResults()) {
      return null;
    }

    if (cursorName != null) {

      ResultBatch resultBatch = resultBatches.get(0);

      //Shouldn't be any actual rows, but release it to any buffers
      resultBatch.release();

      return createResultSet(getCursorName(), resultSetType, resultSetHoldability, resultBatch.getFields());
    }
    else if (query.getStatus() == Query.Status.Completed) {

      ResultBatch resultBatch = resultBatches.get(0);

      // This batch can be re-used so let "close" or "getMoreResults" release it

      return createResultSet(resultBatch.getFields(), resultBatch.getResults(), false, connection.getTypeMap());
    }
    else {
      ResultBatch resultBatch = resultBatches.remove(0);

      // The batch cannot be re-used, but the result set will clean it up when it's
      // done using it

      PGResultSet rs = createResultSet(query, resultBatch.getFields(), resultBatch.getResults());

      // Command cannot be re-used when portal'd batching is in progress
      query = null;

      return rs;
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkClosed();

    if (query == null || !hasUpdateCount()) {
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

    ResultBatch finishedBatch = resultBatches.remove(0);
    finishedBatch.release();

    return hasResults();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();

    if (generatedKeysResultSet == null) {
      return createResultSet(EMPTY_FIELDS, emptyList(), false, connection.getTypeMap());
    }

    return generatedKeysResultSet;
  }

  @Override
  public void cancel() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean isClosed() {
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
  public boolean isWrapperFor(Class<?> iface) {
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
