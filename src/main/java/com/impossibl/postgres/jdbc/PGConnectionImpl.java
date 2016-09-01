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

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.impossibl.postgres.jdbc.Housekeeper.CleanupRunnable;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.ParameterPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.Processor;
import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.DataRow;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;
import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLWarningChain;
import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_CONNECTION;
import static com.impossibl.postgres.jdbc.Exceptions.INVALID_COMMAND_FOR_GENERATED_KEYS;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.jdbc.SQLTextUtils.appendReturningClause;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getBeginText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getCommitText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getGetSessionIsolationLevelText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getGetSessionReadabilityText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getIsolationLevel;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getReleaseSavepointText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getRollbackText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getRollbackToText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getSetSavepointText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getSetSessionIsolationLevelText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.getSetSessionReadabilityText;
import static com.impossibl.postgres.jdbc.SQLTextUtils.isTrue;
import static com.impossibl.postgres.jdbc.SQLTextUtils.prependCursorDeclaration;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.system.Settings.CONNECTION_READONLY;
import static com.impossibl.postgres.system.Settings.DEFAULT_FETCH_SIZE;
import static com.impossibl.postgres.system.Settings.DEFAULT_FETCH_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.NETWORK_TIMEOUT;
import static com.impossibl.postgres.system.Settings.NETWORK_TIMEOUT_DEFAULT;
import static com.impossibl.postgres.system.Settings.PARSED_SQL_CACHE_SIZE;
import static com.impossibl.postgres.system.Settings.PARSED_SQL_CACHE_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.PREPARED_STATEMENT_CACHE_SIZE;
import static com.impossibl.postgres.system.Settings.PREPARED_STATEMENT_CACHE_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.STRICT_MODE;
import static com.impossibl.postgres.system.Settings.STRICT_MODE_DEFAULT;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static java.lang.Boolean.parseBoolean;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;



/**
 * Connection implementation
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGConnectionImpl extends BasicContext implements PGConnection {

  /**
   * Cleans up server resources in the event of leaking connections
   *
   * @author kdubb
   *
   */
  static class Cleanup implements CleanupRunnable {

    Protocol protocol;
    List<WeakReference<PGStatement>> statements;
    Housekeeper.Ref housekeeper;
    StackTraceElement[] allocationStackTrace;

    public Cleanup(Protocol protocol, Housekeeper.Ref housekeeper, List<WeakReference<PGStatement>> statements) {
      this.protocol = protocol;
      this.housekeeper = housekeeper;
      this.statements = statements;
      this.allocationStackTrace = new Exception().getStackTrace();
    }

    @Override
    public String getKind() {
      return "connection";
    }

    @Override
    public StackTraceElement[] getAllocationStackTrace() {
      return allocationStackTrace;
    }

    @Override
    public void run() {

      protocol.shutdown();

      closeStatements(statements);

      housekeeper.release();
    }

  }


  boolean strict;
  long statementId = 0L;
  long portalId = 0L;
  int savepointId;
  private int holdability;
  boolean autoCommit = true;
  int networkTimeout;
  SQLWarning warningChain;
  List<WeakReference<PGStatement>> activeStatements;
  Map<CachedStatementKey, CachedStatement> preparedStatementCache;
  int defaultFetchSize;
  final Housekeeper.Ref housekeeper;
  final Object cleanupKey;

  static Map<String, SQLText> parsedSqlCache;

  PGConnectionImpl(SocketAddress address, Properties settings, Housekeeper.Ref housekeeper) throws IOException, NoticeException {
    super(address, settings, Collections.<String, Class<?>>emptyMap());

    this.strict = getSetting(STRICT_MODE, STRICT_MODE_DEFAULT);
    this.networkTimeout = getSetting(NETWORK_TIMEOUT, NETWORK_TIMEOUT_DEFAULT);
    this.activeStatements = new ArrayList<>();

    final int statementCacheSize = getSetting(PREPARED_STATEMENT_CACHE_SIZE, PREPARED_STATEMENT_CACHE_SIZE_DEFAULT);
    if (statementCacheSize > 0) {
      preparedStatementCache = Collections.synchronizedMap(new LinkedHashMap<CachedStatementKey, CachedStatement>(statementCacheSize + 1, 1.1f, true) {
        private static final long serialVersionUID = 1L;
        @Override
        protected boolean removeEldestEntry(Map.Entry<CachedStatementKey, CachedStatement> eldest) {
          if (size() > statementCacheSize) {
            try {
              PGStatement.dispose(PGConnectionImpl.this, ServerObjectType.Statement, eldest.getValue().name);
            }
            catch (SQLException e) {
              // Ignore...
            }
            return true;
          }
          else {
            return false;
          }
        }
      });
    }

    final int sqlCacheSize = getSetting(PARSED_SQL_CACHE_SIZE, PARSED_SQL_CACHE_SIZE_DEFAULT);
    if (sqlCacheSize > 0) {
      synchronized (PGConnectionImpl.class) {
        if (parsedSqlCache == null) {
          parsedSqlCache = Collections.synchronizedMap(new LinkedHashMap<String, SQLText>(sqlCacheSize + 1, 1.1f, true) {
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, SQLText> eldest) {
              return size() > sqlCacheSize;
            }
          });
        }
      }
    }

    this.defaultFetchSize = getSetting(DEFAULT_FETCH_SIZE, DEFAULT_FETCH_SIZE_DEFAULT);

    this.housekeeper = housekeeper;
    if (this.housekeeper != null)
      this.cleanupKey = this.housekeeper.add(this, new Cleanup(protocol, housekeeper, activeStatements));
    else
      this.cleanupKey = null;
  }

  @Override
  public void init() throws IOException, NoticeException {

    super.init();

    applySettings(settings);
  }

  void applySettings(Properties settings) throws IOException {

    if (parseBoolean(settings.getProperty(CONNECTION_READONLY, "false"))) {
      try {
        setReadOnly(true);
      }
      catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Add warning to end of warning chain
   *
   * @param warning
   */
  public void addWarning(SQLWarning warning) {
    warningChain = chainWarnings(warningChain, warning);
  }

  /**
   * Ensure the connection is not closed
   *
   * @throws SQLException
   *          If the connection is closed
   */
  void checkClosed() throws SQLException {

    if (isClosed())
      throw new SQLException("connection closed", "08006");
  }

  /**
   * Ensures the connection is currently in manual-commit mode
   *
   * @throws SQLException
   *          If the connection is not in manual-commit mode
   */
  void checkManualCommit() throws SQLException {

    if (autoCommit)
      throw new SQLException("must not be in auto-commit mode");
  }

  /**
   * Ensures the connection is currently in auto-commit mode
   *
   * @throws SQLException
   *          IF the connection is not in auto-commit mode
   */
  void checkAutoCommit() throws SQLException {

    if (autoCommit)
      throw new SQLException("must be in auto-commit mode");
  }

  /**
   * Ensures that a transaction is active when in manual commit mode
   *
   * @throws SQLException
   */
  void checkTransaction() throws SQLException {

    if (!autoCommit && protocol.getTransactionStatus() == Idle) {
      execute(getBeginText(), false);
    }

  }

  /**
   * Generates and returns the next unique statement name for this connection
   *
   * @return New unique statement name
   */
  String getNextStatementName() {
    return Long.toHexString(++statementId);
  }

  /**
   * Generates and returns the next unique portal name for this connection
   *
   * @return New unique portal name
   */
  String getNextPortalName() {
    return Long.toHexString(++portalId);
  }

  /**
   * Called by statements to notify the connection of their closure
   *
   * @param statement
   */
  void handleStatementClosure(PGStatement statement) {
    //Remove given & abandoned statements
    Iterator<WeakReference<PGStatement>> statementRefIter = activeStatements.iterator();
    while (statementRefIter.hasNext()) {

      WeakReference<PGStatement> statementRef = statementRefIter.next();
      PGStatement s = statementRef.get();
      if (s == null) {
        statementRefIter.remove();
      }
      else if (s == statement) {
        statementRefIter.remove();
        break;
      }
    }
  }

  /**
   * Closes the given list of result-sets
   *
   * @throws SQLException
   */
  static void closeStatements(List<WeakReference<PGStatement>> statements) {

    for (WeakReference<PGStatement> statementRef : statements) {

      PGStatement statement = statementRef.get();
      if (statement != null) {
        try {
          statement.internalClose();
        }
        catch (SQLException e) {
          //Ignore...
        }
      }

    }

  }

  /**
   * Closes all active statements for this connection
   *
   * @throws SQLException
   */
  void closeStatements() throws SQLException {
    closeStatements(activeStatements);
  }

  SQLText parseSQL(String sqlText) throws SQLException {

    try {
      final boolean standardConformingStrings = getSetting(Settings.STANDARD_CONFORMING_STRINGS, false);

      if (parsedSqlCache == null) {
        return new SQLText(sqlText, standardConformingStrings);
      }

      SQLText parsedSql = parsedSqlCache.get(sqlText);
      if (parsedSql == null) {
        parsedSql = new SQLText(sqlText, standardConformingStrings);
        parsedSqlCache.put(sqlText, parsedSql);
      }

      return parsedSql.copy();
    }
    catch (ParseException e) {
      throw new SQLException("Error parsing SQL at position " + e.getErrorOffset() +
                             " (" + sqlText + "): " + e.getMessage());
    }
  }

  /**
   * Executes the given command and throws a SQLException if an error was
   * encountered and returns a chain of SQLWarnings if any were generated.
   *
   * @param cmd
   *          Command to execute
   * @return Chain of SQLWarning objects if any were encountered
   * @throws SQLException
   *           If an error was encountered
   */
  SQLWarning execute(Command cmd, boolean checkTxn) throws SQLException {

    if (checkTxn) {
      checkTransaction();
    }

    //Enable network timeout
    cmd.setNetworkTimeout(networkTimeout);

    try {

      protocol.execute(cmd);

      if (cmd.getError() != null) {

        throw makeSQLException(cmd.getError());
      }

      return makeSQLWarningChain(cmd.getWarnings());

    }
    catch (BlockingReadTimeoutException e) {

      close();

      throw new SQLTimeoutException(e);
    }
    catch (InterruptedIOException e) {

      close();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {

      if (!protocol.isConnected()) {
        close();
      }

      throw new SQLException(e);
    }

  }

  /**
   * Executes the given SQL text ignoring all result values
   *
   * @param sql
   *          SQL text to execute
   * @throws SQLException
   *           If an error was encountered during execution
   */
  void execute(String sql, boolean checkTxn) throws SQLException {

    if (checkTxn) {
      checkTransaction();
    }

    try {

      query(sql);

    }
    catch (BlockingReadTimeoutException e) {

      internalClose();

      throw new SQLTimeoutException(e);
    }
    catch (InterruptedIOException e) {

      internalClose();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw new SQLException(e);
    }
    catch (NoticeException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw makeSQLException(e.getNotice());
    }

  }

  /**
   * Executes the given SQL text returning the first column of the first row
   *
   * @param sql
   *          SQL text to execute
   * @return String String value of the 1st column of the 1st row or empty
   *         string if no results are available
   * @throws SQLException
   *           If an error was encountered during execution
   */
  String executeForString(String sql, boolean checkTxn) throws SQLException {

    if (checkTxn) {
      checkTransaction();
    }

    try {

      return queryFirstResultString(sql);

    }
    catch (BlockingReadTimeoutException e) {

      internalClose();

      throw new SQLTimeoutException(e);
    }
    catch (InterruptedIOException e) {

      internalClose();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw new SQLException(e);
    }
    catch (NoticeException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw makeSQLException(e.getNotice());
    }

  }

  QueryCommand.ResultBatch executeForFirstResultBatch(String sql, boolean checkTxn, Object... params) throws SQLException {

    if (checkTxn) {
      checkTransaction();
    }

    try {

      return queryBatch(sql, params);

    }
    catch (BlockingReadTimeoutException e) {

      internalClose();

      throw new SQLTimeoutException(e);
    }
    catch (InterruptedIOException e) {

      internalClose();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw new SQLException(e);
    }
    catch (NoticeException e) {

      if (!protocol.isConnected()) {
        internalClose();
      }

      throw makeSQLException(e.getNotice());
    }

  }

  DataRow executeForFirstResult(String sql, boolean checkTxn, Object... params) throws SQLException {

    QueryCommand.ResultBatch resultBatch = executeForFirstResultBatch(sql, checkTxn, params);

    List<DataRow> res = resultBatch.results;
    if (res == null || res.isEmpty())
      return null;

    DataRow firstResult = res.remove(0);

    resultBatch.release();

    return firstResult;
  }

  <T> T executeForFirstResultValue(String sql, boolean checkTxn, Class<T> returnType, Object... params) throws SQLException {

    DataRow result = executeForFirstResult(sql, checkTxn, params);
    if (result == null)
      return null;

    try {
      return returnType.cast(result.getColumn(0));
    }
    catch (IOException e) {
      throw new SQLException("Error decoding column", e);
    }
    finally {
      result.release();
    }

  }

  long executeForRowsAffected(String sql, boolean checkTxn, Object... params) throws SQLException {

    QueryCommand.ResultBatch resultBatch = executeForFirstResultBatch(sql, checkTxn, params);

    resultBatch.release();

    return resultBatch.rowsAffected;
  }

  /**
   * {@inheritDoc}
   */
  public void setStrictMode(boolean v) {
    strict = v;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isStrictMode() {
    return strict;
  }

  /**
   * {@inheritDoc}
   */
  public void setDefaultFetchSize(int v) {
    defaultFetchSize = v;
  }

  /**
   * {@inheritDoc}
   */
  public int getDefaultFetchSize() {
    return defaultFetchSize;
  }

  /**
   * Closes all statements and shuts down the protocol
   *
   * @throws SQLException If an error occurs closing any of the statements
   */
  void internalClose() throws SQLException {

    closeStatements();

    shutdown();

    notificationListeners.clear();

    if (housekeeper != null) {
      housekeeper.remove(cleanupKey);
      housekeeper.release();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isServerMinimumVersion(int major, int minor) {
    return getServerVersion().isMinimum(major, minor);
  }

  @Override
  public synchronized boolean isValid(int timeout) throws SQLException {

    //Not valid if connection is closed
    if (isClosed())
      return false;

    if (timeout < 0)
       throw new SQLException("Timeout is less than 0");

    boolean result = true;
    int origNetworkTimeout = networkTimeout;
    try {
      networkTimeout = timeout * 1000;
      result = executeForString("SELECT '1'::char", false).equals("1");
    }
    catch (SQLException se) {
      result = false;
    }
    networkTimeout = origNetworkTimeout;

    return result;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();

    return targetTypeMap;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
    checkClosed();

    targetTypeMap = unmodifiableMap(typeMap);
  }

  @Override
  public int getHoldability() throws SQLException {
    checkClosed();

    return holdability;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkClosed();

    if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT &&
        holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
      throw new SQLException("illegal argument");
    }

    this.holdability = holdability;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    return new PGDatabaseMetaData(this);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkClosed();

    return autoCommit;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();

    // Do nothing if no change in state
    if (this.autoCommit == autoCommit)
      return;

    // Commit any in-flight transaction (cannot call commit as it will start a
    // new transaction since we would still be in manual commit mode)
    if (!this.autoCommit && protocol.getTransactionStatus() != Idle) {
      execute(getCommitText(), false);
    }

    this.autoCommit = autoCommit;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();

    String readability = executeForString(getGetSessionReadabilityText(), false);

    return isTrue(readability);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();

    if (protocol.getTransactionStatus() != Idle) {
      throw new SQLException("cannot set read only during a transaction");
    }

    execute(getSetSessionReadabilityText(readOnly), false);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkClosed();

    String isolLevel = executeForString(getGetSessionIsolationLevelText(), false);

    return getIsolationLevel(isolLevel);
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();

    if (level != Connection.TRANSACTION_NONE &&
        level != Connection.TRANSACTION_READ_UNCOMMITTED &&
        level != Connection.TRANSACTION_READ_COMMITTED &&
        level != Connection.TRANSACTION_REPEATABLE_READ &&
        level != Connection.TRANSACTION_SERIALIZABLE) {
      throw new SQLException("illegal argument");
    }

    execute(getSetSessionIsolationLevelText(level), false);
  }

  @Override
  public void commit() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Commit the current transaction
    if (protocol.getTransactionStatus() != Idle) {
      execute(getCommitText(), false);
    }

  }

  @Override
  public void rollback() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Roll back the current transaction
    if (protocol.getTransactionStatus() != Idle) {
      execute(getRollbackText(), false);
    }

  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Allocate new save-point name & wrapper
    PGSavepoint savepoint = new PGSavepoint(++savepointId);

    // Mark save-point
    execute(getSetSavepointText(savepoint), true);

    return savepoint;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    checkManualCommit();

    // Allocate new save-point wrapper
    PGSavepoint savepoint = new PGSavepoint(name);

    // Mark save-point
    execute(getSetSavepointText(savepoint), true);

    return savepoint;
  }

  @Override
  public void rollback(Savepoint savepointParam) throws SQLException {
    checkClosed();
    checkManualCommit();

    PGSavepoint savepoint = (PGSavepoint) savepointParam;

    if (!savepoint.isValid()) {
      throw new SQLException("invalid savepoint");
    }

    try {
      // Rollback to save-point (if in transaction)
      if (protocol.getTransactionStatus() != Idle) {
        execute(getRollbackToText(savepoint), false);
      }
    }
    finally {
      // Mark as released
      savepoint.setReleased(true);
    }

  }

  @Override
  public void releaseSavepoint(Savepoint savepointParam) throws SQLException {
    checkClosed();
    checkManualCommit();

    PGSavepoint savepoint = (PGSavepoint) savepointParam;

    if (!savepoint.isValid()) {
      throw new SQLException("invalid savepoint");
    }

    try {
      // Release the save-point (if in a transaction)
      if (!savepoint.getReleased() && protocol.getTransactionStatus() != Idle) {
        execute(getReleaseSavepointText(savepoint), false);
      }
    }
    finally {
      // Use up the save-point
      savepoint.invalidate();
    }

  }

  @Override
  public String getCatalog() throws SQLException {
    checkClosed();
    return null;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkClosed();
  }

  @Override
  public String getSchema() throws SQLException {
    checkClosed();
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    checkClosed();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    SQLTextEscapes.processEscapes(sqlText, this);

    return sqlText.toString();
  }

  @Override
  public PGStatement createStatement() throws SQLException {

    return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PGStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

    return createStatement(resultSetType, resultSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PGStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkClosed();

    PGSimpleStatement statement = new PGSimpleStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);

    activeStatements.add(new WeakReference<PGStatement>(statement));

    return statement;
  }

  @Override
  public PGPreparedStatement prepareStatement(String sql) throws SQLException {

    return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PGPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

    return prepareStatement(sql, resultSetType, resultSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PGPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    return prepareStatement(sqlText, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  public PGPreparedStatement prepareStatement(SQLText sqlText, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    SQLTextEscapes.processEscapes(sqlText, this);

    String statementName = getNextStatementName();

    String cursorName = null;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {

      cursorName = "cursor" + statementName;

      if (!prependCursorDeclaration(sqlText, cursorName, resultSetType, resultSetHoldability, autoCommit)) {

        cursorName = null;

      }

    }

    final int[] parameterCount = new int[1];
    sqlText.process(new Processor() {

      @Override
      public Node process(Node node) throws SQLException {
        if (node instanceof ParameterPiece)
          parameterCount[0] += 1;
        return node;
      }

    }, true);

    if (parameterCount[0] > 0xffff) {
      throw new PGSQLSimpleException("Too many parameters specified: Max of 65535 allowed");
    }

    PGPreparedStatement statement =
        new PGPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, statementName, sqlText.toString(), parameterCount[0], cursorName);

    activeStatements.add(new WeakReference<PGStatement>(statement));

    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    if (autoGeneratedKeys != RETURN_GENERATED_KEYS) {
      return prepareStatement(sql);
    }

    if (!appendReturningClause(sqlText)) {
      throw INVALID_COMMAND_FOR_GENERATED_KEYS;
    }

    PGPreparedStatement statement = prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);

    statement.setWantsGeneratedKeys(true);

    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    if (!appendReturningClause(sqlText, asList(columnNames))) {
      throw INVALID_COMMAND_FOR_GENERATED_KEYS;
    }

    PGPreparedStatement statement = prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);

    statement.setWantsGeneratedKeys(true);

    return statement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return prepareCall(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return prepareCall(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, getHoldability());
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    return prepareCall(sqlText, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  public PGCallableStatement prepareCall(SQLText sqlText, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    final int[] parameterCount = new int[1];
    Processor counter = new Processor() {

      @Override
      public Node process(Node node) throws SQLException {
        if (node instanceof ParameterPiece)
          parameterCount[0] += 1;
        return node;
      }

    };

    sqlText.process(counter, true);
    int preParameterCount = parameterCount[0];

    SQLTextEscapes.processEscapes(sqlText, this);

    parameterCount[0] = 0;
    sqlText.process(counter, true);
    int finalParameterCount = parameterCount[0];

    String statementName = getNextStatementName();

    String cursorName = null;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {

      cursorName = "cursor" + statementName;

      if (!prependCursorDeclaration(sqlText, cursorName, resultSetType, resultSetHoldability, autoCommit)) {

        cursorName = null;

      }

    }

    boolean hasAssign = preParameterCount == (finalParameterCount + 1);

    PGCallableStatement statement =
        new PGCallableStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, statementName, sqlText.toString(), parameterCount[0], cursorName, hasAssign);

    activeStatements.add(new WeakReference<PGStatement>(statement));

    return statement;
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkClosed();

    int loOid = LargeObject.creat(this, 0);

    return new PGBlob(this, loOid);
  }

  @Override
  public Clob createClob() throws SQLException {
    checkClosed();

    int loOid = LargeObject.creat(this, 0);

    return new PGClob(this, loOid);
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    checkClosed();

    return new PGSQLXML(this);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkClosed();

    Type type = getRegistry().loadType(typeName + "[]");
    if (type == null) {
      throw new SQLException("Array type not found");
    }

    return new PGArray(this, (ArrayType)type, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();

    Type type = getRegistry().loadType(typeName);
    if (!(type instanceof CompositeType)) {
      throw new SQLException("Invalid type for struct");
    }

    CompositeType compositeType = (CompositeType) type;
    return new PGStruct(this, type.getName(), compositeType.getAttributesTypes(), attributes);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return !protocol.isConnected();
  }

  @Override
  public void close() throws SQLException {

    // Ignore multiple closes
    if (isClosed())
      return;

    internalClose();
  }

  @Override
  public void abort(Executor executor) throws SQLException {

    getProtocol().abort(executor);

    shutdown();

    if (housekeeper != null)
      housekeeper.remove(cleanupKey);
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
  public int getNetworkTimeout() throws SQLException {
    checkClosed();
    return networkTimeout;
  }

  @Override
  public void setNetworkTimeout(Executor executor, int networkTimeout) throws SQLException {
    checkClosed();

    if (networkTimeout < 0) {
      throw new SQLException("invalid network timeout");
    }

    this.networkTimeout = networkTimeout;
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

  @Override
  public void addNotificationListener(String name, String channelNameFilter, PGNotificationListener listener) {
    super.addNotificationListener(name, channelNameFilter, listener);
  }

  @Override
  public void addNotificationListener(String channelNameFilter, PGNotificationListener listener) {
    super.addNotificationListener(null, channelNameFilter, listener);
  }

  @Override
  public void addNotificationListener(PGNotificationListener listener) {
    super.addNotificationListener(null, null, listener);
  }

  @Override
  public void removeNotificationListener(PGNotificationListener listener) {
    super.removeNotificationListener(listener);
  }

  boolean isCacheEnabled() {
    return preparedStatementCache != null;
  }

  CachedStatement getCachedStatement(CachedStatementKey key, Callable<CachedStatement> loader) throws Exception {

    if (preparedStatementCache == null) {
      return loader.call();
    }

    CachedStatement cached = preparedStatementCache.get(key);
    if (cached == null) {

      cached = loader.call();

      preparedStatementCache.put(key, cached);
    }

    return cached;
  }

}

class CachedStatementKey {

  String sql;
  List<Type> parameterTypes;

  public CachedStatementKey(String sql, List<Type> parameterTypes) {
    this.sql = sql;
    this.parameterTypes = parameterTypes;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((parameterTypes == null) ? 0 : parameterTypes.hashCode());
    result = prime * result + ((sql == null) ? 0 : sql.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CachedStatementKey other = (CachedStatementKey) obj;
    if (parameterTypes == null) {
      if (other.parameterTypes != null)
        return false;
    }
    else if (!parameterTypes.equals(other.parameterTypes))
      return false;
    if (sql == null) {
      if (other.sql != null)
        return false;
    }
    else if (!sql.equals(other.sql))
      return false;
    return true;
  }

}

class CachedStatement {

  String name;
  List<Type> parameterTypes;
  List<ResultField> resultFields;

  public CachedStatement(String statementName, List<Type> parameterTypes, List<ResultField> resultFields) {
    this.name = statementName;
    this.parameterTypes = parameterTypes;
    this.resultFields = resultFields;
  }

}
