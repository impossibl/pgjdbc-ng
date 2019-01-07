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
import com.impossibl.postgres.jdbc.Housekeeper.CleanupRunnable;
import com.impossibl.postgres.jdbc.SQLTextTree.ParameterPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.Processor;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.ServerConnection;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.SharedRegistry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;
import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_CONNECTION;
import static com.impossibl.postgres.jdbc.Exceptions.INVALID_COMMAND_FOR_GENERATED_KEYS;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_FETCH_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_NETWORK_TIMEOUT;
import static com.impossibl.postgres.jdbc.JDBCSettings.DESCRIPTION_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.JDBC;
import static com.impossibl.postgres.jdbc.JDBCSettings.PARSED_SQL_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.PREPARED_STATEMENT_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.PREPARED_STATEMENT_CACHE_THRESHOLD;
import static com.impossibl.postgres.jdbc.JDBCSettings.READ_ONLY;
import static com.impossibl.postgres.jdbc.JDBCSettings.STRICT_MODE;
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
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_URL;
import static com.impossibl.postgres.system.SystemSettings.PROTO;
import static com.impossibl.postgres.system.SystemSettings.SERVER;
import static com.impossibl.postgres.system.SystemSettings.STANDARD_CONFORMING_STRINGS;
import static com.impossibl.postgres.system.SystemSettings.SYS;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;


/**
 * Direct connection implementation
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGDirectConnection extends BasicContext implements PGConnection {

  private static final Logger logger = Logger.getLogger(PGDirectConnection.class.getName());

  /**
   * Cleans up server resources in the event of leaking connections
   *
   * @author kdubb
   *
   */
  private static class Cleanup implements CleanupRunnable {

    ServerConnection serverConnection;
    List<WeakReference<PGStatement>> statements;
    StackTraceElement[] allocationStackTrace;
    String connectionInfo;

    private Cleanup(ServerConnection serverConnection, List<WeakReference<PGStatement>> statements, String connectionInfo) {
      this.serverConnection = serverConnection;
      this.statements = statements;
      this.allocationStackTrace = new Exception().getStackTrace();
      this.connectionInfo = connectionInfo;
    }

    @Override
    public String getKind() {
      return "connection ( " + connectionInfo + " )";
    }

    @Override
    public StackTraceElement[] getAllocationStackTrace() {
      return allocationStackTrace;
    }

    @Override
    public void run() {

      serverConnection.shutdown();

      closeStatements(statements);
    }

  }

  boolean strict;
  private long statementId = 0L;
  private long portalId = 0L;
  private int savepointId;
  private int holdability;
  boolean autoCommit = true;
  private int networkTimeout;
  private SQLWarning warningChain;
  private List<WeakReference<PGStatement>> activeStatements;
  private Map<StatementCacheKey, StatementDescription> descriptionCache;
  private Map<StatementCacheKey, PreparedStatementDescription> preparedStatementCache;
  private int preparedStatementCacheThreshold;
  private Map<StatementCacheKey, Integer> preparedStatementHeat;
  private Integer defaultFetchSize;
  private Map<NotificationKey, PGNotificationListener> notificationListeners;
  final Housekeeper.Ref housekeeper;
  private final Object cleanupKey;

  private static Map<String, SQLText> parsedSqlCache;

  PGDirectConnection(SocketAddress address, Settings settings, Housekeeper.Ref housekeeper) throws IOException {
    super(address, settings.duplicateKnowing(JDBC, SYS, PROTO, SERVER));

    this.strict = getSetting(STRICT_MODE);
    this.networkTimeout = getSetting(DEFAULT_NETWORK_TIMEOUT);
    this.activeStatements = new ArrayList<>();
    this.notificationListeners = new ConcurrentHashMap<>();

    final int descriptionCacheSize = getSetting(DESCRIPTION_CACHE_SIZE);
    if (descriptionCacheSize > 0) {
      this.descriptionCache = Collections.synchronizedMap(new LinkedHashMap<StatementCacheKey, StatementDescription>(descriptionCacheSize + 1, 1.1f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<StatementCacheKey, StatementDescription> eldest) {
          return size() > descriptionCacheSize;
        }
      });
    }

    final int statementCacheSize = getSetting(PREPARED_STATEMENT_CACHE_SIZE);
    if (statementCacheSize > 0) {
      preparedStatementCache = Collections.synchronizedMap(new LinkedHashMap<StatementCacheKey, PreparedStatementDescription>(statementCacheSize + 1, 1.1f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<StatementCacheKey, PreparedStatementDescription> eldest) {
          if (size() > statementCacheSize) {
            try {
              PGStatement.dispose(PGDirectConnection.this, eldest.getValue().name);
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

    final int statementCacheThreshold = getSetting(PREPARED_STATEMENT_CACHE_THRESHOLD);
    if (statementCacheThreshold > 0) {
      preparedStatementCacheThreshold = statementCacheThreshold;
      preparedStatementHeat = new ConcurrentHashMap<>();
    }

    final int sqlCacheSize = getSetting(PARSED_SQL_CACHE_SIZE);
    if (sqlCacheSize > 0) {
      synchronized (PGDirectConnection.class) {
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

    this.defaultFetchSize = getSetting(DEFAULT_FETCH_SIZE);

    prepareUtilQuery("TB", getBeginText());
    prepareUtilQuery("TC", getCommitText());
    prepareUtilQuery("TR", getRollbackText());

    this.housekeeper = housekeeper;
    if (this.housekeeper != null)
      this.cleanupKey = this.housekeeper.add(this, new Cleanup(serverConnection, activeStatements, getSetting(DATABASE_URL)));
    else
      this.cleanupKey = null;
  }

  @Override
  public void init(SharedRegistry.Factory sharedRegistryFactory) throws IOException {

    super.init(sharedRegistryFactory);

    applySettings(settings);
  }

  private void applySettings(Settings settings) throws IOException {

    if (settings.enabled(READ_ONLY)) {
      try {
        setReadOnly(true);
      }
      catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  public TransactionStatus getTransactionStatus() throws SQLException {
    try {
      return serverConnection.getTransactionStatus();
    }
    catch (ClosedChannelException e) {
      internalClose();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {
      internalClose();

      throw new PGSQLSimpleException(e);
    }
  }

  /**
   * Add warning to end of warning chain
   *
   * @param warning Warning to add
   */
  void addWarning(SQLWarning warning) {
    warningChain = chainWarnings(warningChain, warning);
  }

  /**
   * Ensure the connection is not closed
   *
   * @throws SQLException If the connection is closed
   */
  void checkClosed() throws SQLException {

    if (isClosed())
      throw new SQLException("connection closed", "08006");
  }

  /**
   * Ensures the connection is currently in manual-commit mode
   *
   * @throws SQLException If the connection is not in manual-commit mode
   */
  private void checkManualCommit() throws SQLException {

    if (autoCommit)
      throw new SQLException("must not be in auto-commit mode");
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
   * @param statement Closed statement
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
   * Closes the given list of statements
   *
   * @param statements Statements to close
   */
  private static void closeStatements(List<WeakReference<PGStatement>> statements) {

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
   */
  private void closeStatements() {
    closeStatements(activeStatements);
    activeStatements.clear();
  }

  SQLText parseSQL(String sqlText) throws SQLException {

    try {
      final boolean standardConformingStrings = getSetting(STANDARD_CONFORMING_STRINGS, false);

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



  interface QueryFunction {
    void query(long timeout) throws IOException;
  }

  /**
   * Executes the given function, mapping exceptions/errors to their JDBC counterparts and
   * initiating any necessary side effects (e.g. closing the connection).
   *
   * @param function Query function to execute
   * @throws SQLException If an error was encountered during execution
   */

  void execute(QueryFunction function) throws SQLException {
    execute((timeout) -> {
      function.query(timeout);
      return null;
    });
  }



  interface QueryResultFunction<T> {

    T query(long timeout) throws IOException;

  }

  /**
   * Executes the given function, mapping exceptions/errors to their JDBC counterparts and
   * initiating any necessary side effects (e.g. closing the connection).
   *
   * @param function Query function to execute
   * @throws SQLException If an error was encountered during execution
   */
  <T> T execute(QueryResultFunction<T> function) throws SQLException {

    try {
      if (!autoCommit && serverConnection.getTransactionStatus() == Idle) {
        serverConnection.getRequestExecutor().lazyExecute("TB");
      }

      return function.query(networkTimeout);

    }
    catch (BlockingReadTimeoutException e) {

      internalClose();

      throw new SQLTimeoutException(e);
    }
    catch (InterruptedIOException | ClosedChannelException e) {

      internalClose();

      throw CLOSED_CONNECTION;
    }
    catch (IOException e) {

      if (!serverConnection.isConnected()) {
        internalClose();
      }

      throw makeSQLException(e);
    }

  }

  <T> T executeTimed(Long executionTimeout, QueryResultFunction<T> function) throws SQLException {

    if (executionTimeout == null || executionTimeout < 1 || (networkTimeout > 0 && networkTimeout < executionTimeout)) {
      return execute(function);
    }

    // Lock the executor to ensure no asynchronous requests are
    // started while we're operating under the execution timeout.
    // This ensures we don't cancel a request _after_ this one
    // by mistake.

    synchronized (serverConnection.getRequestExecutor()) {

      // Schedule task to run at execution timeout

      ExecutionTimerTask task = new CancelRequestTask(serverConnection.getRemoteAddress(), getKeyData());

      ScheduledFuture<?> taskHandle = serverConnection.getIOExecutor().schedule(task, executionTimeout, MILLISECONDS);

      try {

        return execute(function);

      }
      finally {

        // Cancel the scheduled running (if it hasn't began to run)
        taskHandle.cancel(true);

        // Also, ensure any task that is currently running also gets
        // completely cancelled, or finishes, before returning
        task.cancel();

      }

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
  void execute(String sql) throws SQLException {

    execute((long timeout) -> query(sql, timeout));
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
  String executeForString(String sql) throws SQLException {

    return execute((long timeout) -> queryString(sql, timeout));
  }

  private ResultBatch executeForResultBatch(String sql) throws SQLException {

    return execute((long timeout) -> queryBatch(sql, timeout));
  }

  private ResultBatch executeForResultBatch(String sql, Object[] params) throws SQLException {

    return execute((long timeout) -> queryBatchPrepared(sql, params, timeout));
  }

  private ResultBatch executeForResultBatch(String sql, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers) throws SQLException {

    return execute((long timeout) -> queryBatchPrepared(sql, parameterFormats, parameterBuffers, timeout));
  }

  RowData executeForResult(String sql) throws SQLException {

    try (ResultBatch resultBatch = executeForResultBatch(sql)) {
      if (resultBatch.isEmpty()) {
        return null;
      }

      return resultBatch.borrowRows().take(0);
    }
  }

  <T> T executeForValue(String sql, Class<T> returnType, Object... params) throws SQLException {

    try (ResultBatch resultBatch = executeForResultBatch(sql, params)) {

      try {
        Object value = resultBatch.borrowRows().borrow(0).getField(0, resultBatch.getFields()[0], this, returnType, null);
        return returnType.cast(value);
      }
      catch (IOException e) {
        throw new SQLException("Error decoding column", e);
      }
    }

  }

  long executeForRowsAffected(String sql) throws SQLException {

    try (ResultBatch resultBatch = executeForResultBatch(sql)) {
      return firstNonNull(resultBatch.getRowsAffected(), 0L);
    }
  }

  long executeForRowsAffected(String sql, FieldFormatRef[] paramFormats, ByteBuf[] paramBuffers) throws SQLException {

    try (ResultBatch resultBatch = executeForResultBatch(sql, paramFormats, paramBuffers)) {
      return firstNonNull(resultBatch.getRowsAffected(), 0L);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStrictMode(boolean v) {
    strict = v;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStrictMode() {
    return strict;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setDefaultFetchSize(Integer v) {
    defaultFetchSize = v;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getDefaultFetchSize() {
    return defaultFetchSize;
  }

  @Override
  public PGAnyType resolveType(String name) throws SQLException {
    try {
      Type type = registry.loadTransientType(name);
      return new PGResolvedType(type);
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  /**
   * Release all resources and shut down the protocol using network timeout
   *
   */
  private void internalClose() {

    closeStatements();

    shutdown().awaitUninterruptibly(networkTimeout > 0 ? networkTimeout : Integer.MAX_VALUE);

    reportClosed();
    notificationListeners.clear();

    if (housekeeper != null) {
      housekeeper.remove(cleanupKey);
      housekeeper.release();
    }
  }

  @Override
  protected void connectionClosed() {
    internalClose();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isServerMinimumVersion(int major, int minor) {
    return serverConnection.getServerInfo().getVersion().isMinimum(major, minor);
  }

  @Override
  public synchronized boolean isValid(int timeout) throws SQLException {

    //Not valid if connection is closed
    if (isClosed())
      return false;

    if (timeout < 0)
      throw new SQLException("Timeout is less than 0");

    boolean result;
    int origNetworkTimeout = networkTimeout;
    try {
      networkTimeout = (int) SECONDS.toMillis(timeout);
      execute("SELECT '1'::char");
      result = true;
    }
    catch (Exception se) {
      result = false;
    }
    networkTimeout = origNetworkTimeout;

    return result;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();

    return getCustomTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
    checkClosed();

    this.typeMap = unmodifiableMap(typeMap);
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
    if (!this.autoCommit && getTransactionStatus() != Idle) {
      execute(getCommitText());
    }

    this.autoCommit = autoCommit;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();

    String readability = executeForString(getGetSessionReadabilityText());

    return isTrue(readability);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();

    if (getTransactionStatus() != Idle) {
      throw new SQLException("cannot set read only during a transaction");
    }

    execute(getSetSessionReadabilityText(readOnly));
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkClosed();

    String isolLevel = executeForString(getGetSessionIsolationLevelText());

    //noinspection MagicConstant
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

    execute(getSetSessionIsolationLevelText(level));
  }

  @Override
  public void commit() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Commit the current transaction
    if (getTransactionStatus() != Idle) {
      execute("@TC");
    }

  }

  @Override
  public void rollback() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Roll back the current transaction
    if (getTransactionStatus() != Idle) {
      execute("@TR");
    }

  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Allocate new save-point name & wrapper
    PGSavepoint savepoint = new PGSavepoint(++savepointId);

    // Mark save-point
    execute(getSetSavepointText(savepoint));

    return savepoint;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    checkManualCommit();

    // Allocate new save-point wrapper
    PGSavepoint savepoint = new PGSavepoint(name);

    // Mark save-point
    execute(getSetSavepointText(savepoint));

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
      if (getTransactionStatus() != Idle) {

        execute(getRollbackToText(savepoint));

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
      if (!savepoint.getReleased() && getTransactionStatus() != Idle) {
        execute(getReleaseSavepointText(savepoint));
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

    activeStatements.add(new WeakReference<>(statement));

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

    String cursorName = null;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {

      cursorName = "cursor" + getNextStatementName();

      if (!prependCursorDeclaration(sqlText, cursorName, resultSetType, resultSetHoldability, autoCommit)) {

        cursorName = null;

      }

    }

    final int[] parameterCount = new int[1];
    sqlText.process(node -> {
      if (node instanceof ParameterPiece)
        parameterCount[0] += 1;
      return node;
    }, true);

    if (parameterCount[0] > 0xffff) {
      throw new PGSQLSimpleException("Too many parameters specified: Max of 65535 allowed");
    }

    PGPreparedStatement statement =
        new PGPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, sqlText.toString(), parameterCount[0], cursorName);

    activeStatements.add(new WeakReference<>(statement));

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

    statement.setWantsGeneratedKeys();

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

    statement.setWantsGeneratedKeys();

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

  private PGCallableStatement prepareCall(SQLText sqlText, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    final int[] parameterCount = new int[1];
    Processor counter = node -> {
      if (node instanceof ParameterPiece)
        parameterCount[0] += 1;
      return node;
    };

    sqlText.process(counter, true);
    int preParameterCount = parameterCount[0];

    SQLTextEscapes.processEscapes(sqlText, this);

    parameterCount[0] = 0;
    sqlText.process(counter, true);
    int finalParameterCount = parameterCount[0];

    String cursorName = null;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {

      cursorName = "cursor" + getNextStatementName();

      if (!prependCursorDeclaration(sqlText, cursorName, resultSetType, resultSetHoldability, autoCommit)) {

        cursorName = null;

      }

    }

    boolean hasAssign = preParameterCount == (finalParameterCount + 1);

    PGCallableStatement statement =
        new PGCallableStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, sqlText.toString(), parameterCount[0], cursorName, hasAssign);

    activeStatements.add(new WeakReference<>(statement));

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

    try {

      Type elementType = getRegistry().loadTransientType(typeName);
      if (elementType == null) {
        throw new PGSQLSimpleException("Unknown element type");
      }

      Type type = getRegistry().loadType(elementType.getArrayTypeId());
      if (!(type instanceof ArrayType)) {
        throw new SQLException("Array type not found");
      }

      return PGBuffersArray.encode(this, (ArrayType) type, elements);
    }
    catch (IOException e) {
      throw makeSQLException("Error encoding array values", e);
    }
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();

    try {

      Type type = getRegistry().loadTransientType(typeName);
      if (!(type instanceof CompositeType)) {
        throw new SQLException("Invalid type for struct");
      }

      return PGBuffersStruct.Binary.encode(this, (CompositeType) type, attributes);
    }
    catch (IOException e) {
      throw makeSQLException("Error encoding struct", e);
    }
  }

  @Override
  public void setClientInfo(String name, String value) {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(Properties properties) {
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
  public boolean isClosed() {
    return !serverConnection.isConnected();
  }

  @Override
  public void close() {

    // Ignore multiple closes
    if (isClosed())
      return;

    internalClose();
  }

  @Override
  public void abort(Executor executor) {

    if (isClosed())
      return;

    // Save socket address (as shutdown might erase it)
    SocketAddress serverAddress = serverConnection.getRemoteAddress();

    //Shutdown socket (also guarantees no more commands begin execution)
    ChannelFuture shutdown = shutdown();

    //Issue cancel request from separate socket (per Postgres protocol). This
    //is a convenience to the server as the abort does not depend on its
    //success to complete properly

    executor.execute(new CancelRequestTask(serverAddress, getKeyData()));

    shutdown.syncUninterruptibly();

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
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  protected void connectionNotificationReceived(int processId, String channelName, String payload) {
    reportNotification(processId, channelName, payload);
  }

  @Override
  public void addNotificationListener(PGNotificationListener listener) {
    addNotificationListener(null, null, listener);
  }

  @Override
  public void addNotificationListener(String channelNameFilter, PGNotificationListener listener) {
    addNotificationListener(null, channelNameFilter, listener);
  }

  public void addNotificationListener(String name, String channelNameFilter, PGNotificationListener listener) {

    name = nullToEmpty(name);
    channelNameFilter = channelNameFilter != null ? channelNameFilter : ".*";

    Pattern channelNameFilterPattern = Pattern.compile(channelNameFilter);

    NotificationKey key = new NotificationKey(name, channelNameFilterPattern);

    notificationListeners.put(key, listener);
  }

  public void removeNotificationListener(PGNotificationListener listener) {

    Iterator<Map.Entry<NotificationKey, PGNotificationListener>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, PGNotificationListener> entry = iter.next();

      PGNotificationListener iterListener = entry.getValue();
      if (iterListener == null || iterListener.equals(listener)) {

        iter.remove();
      }

    }
  }

  public void removeNotificationListener(String listenerName) {

    Iterator<Map.Entry<NotificationKey, PGNotificationListener>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, PGNotificationListener> entry = iter.next();

      String iterListenerName = entry.getKey().name;
      PGNotificationListener iterListener = entry.getValue();
      if (iterListenerName.equals(listenerName) || iterListener == null) {

        iter.remove();
      }

    }
  }

  private void reportNotification(int processId, String channelName, String payload) {

    for (Map.Entry<NotificationKey, PGNotificationListener> entry : notificationListeners.entrySet()) {

      PGNotificationListener listener = entry.getValue();
      if (entry.getKey().channelNameFilter.matcher(channelName).matches()) {

        try {
          listener.notification(processId, channelName, payload);
        }
        catch (Throwable t) {
          logger.log(Level.WARNING, "Exception in connection listener", t);
        }
      }

    }

  }

  private void reportClosed() {

    for (Map.Entry<NotificationKey, PGNotificationListener> entry : notificationListeners.entrySet()) {

      PGNotificationListener listener = entry.getValue();

      try {
        listener.closed();
      }
      catch (Throwable t) {
        logger.log(Level.WARNING, "Exception in connection listener", t);
      }
    }

  }

  boolean isCacheEnabled() {
    return preparedStatementCache != null;
  }

  interface StatementDescriptionLoader {
    StatementDescription load() throws IOException, SQLException;
  }

  StatementDescription getCachedStatementDescription(String sql, StatementDescriptionLoader loader) throws SQLException {

    StatementCacheKey key = new StatementCacheKey(sql, EMPTY_TYPES);

    // Check prepared statement cache...
    if (preparedStatementCache != null) {
      PreparedStatementDescription cached = preparedStatementCache.get(key);
      if (cached != null) return cached;
    }

    // Check description cache
    StatementDescription cached = descriptionCache.get(key);
    if (cached != null) return cached;

    try {
      cached = loader.load();
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }

    descriptionCache.put(key, cached);

    return cached;
  }


  interface PreparedStatementDescriptionLoader {
    PreparedStatementDescription load() throws IOException, SQLException;
  }

  PreparedStatementDescription getCachedPreparedStatement(StatementCacheKey key, PreparedStatementDescriptionLoader loader) throws SQLException {

    if (preparedStatementCache == null) {
      try {
        return loader.load();
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }
    }

    PreparedStatementDescription cached = preparedStatementCache.get(key);
    if (cached != null) return cached;


    if (preparedStatementHeat != null) {
      Integer heat = preparedStatementHeat.computeIfPresent(key, (k, h) -> h + 1);
      if (heat == null) {
        preparedStatementHeat.put(key, 1);
        return null;
      }
      else if (heat < preparedStatementCacheThreshold) {
        return null;
      }
    }

    try {
      cached = loader.load();
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }

    preparedStatementCache.put(key, cached);

    // Save a copy in the description cache as well. This cache uses no parameter types for
    // more general lookup capability.
    descriptionCache.putIfAbsent(new StatementCacheKey(key.getSql(), EMPTY_TYPES), cached);

    return cached;
  }

}

class StatementCacheKey {

  private String sql;
  private Type[] parameterTypes;

  StatementCacheKey(String sql, Type[] parameterTypes) {
    this.sql = sql;
    this.parameterTypes = parameterTypes.clone();
  }

  public String getSql() {
    return sql;
  }

  public Type[] getParameterTypes() {
    return parameterTypes;
  }

  @Override
  public String toString() {
    return "CachedStatementKey{" +
        "sql='" + sql + '\'' +
        ", parameterTypes=" + Arrays.toString(parameterTypes) +
        '}';
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(sql);
    result = 31 * result + Arrays.hashCode(parameterTypes);
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
    StatementCacheKey other = (StatementCacheKey) obj;
    if (parameterTypes == null) {
      if (other.parameterTypes != null)
        return false;
    }
    else if (!Arrays.equals(parameterTypes, other.parameterTypes))
      return false;
    if (sql == null) {
      return other.sql == null;
    }
    else return sql.equals(other.sql);
  }

}

class StatementDescription {

  Type[] parameterTypes;
  ResultField[] resultFields;

  StatementDescription(Type[] parameterTypes, ResultField[] resultFields) {
    this.parameterTypes = parameterTypes;
    this.resultFields = resultFields;
  }

}

class PreparedStatementDescription extends StatementDescription {

  String name;

  PreparedStatementDescription(String statementName, Type[] parameterTypes, ResultField[] resultFields) {
    super(parameterTypes, resultFields);
    this.name = statementName;
  }

}

class NotificationKey {

  String name;
  Pattern channelNameFilter;

  NotificationKey(String name, Pattern channelNameFilter) {
    this.name = name;
    this.channelNameFilter = channelNameFilter;
  }

  String getName() {
    return name;
  }

}
