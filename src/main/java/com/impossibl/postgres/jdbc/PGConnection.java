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
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.system.Settings.CONNECTION_READONLY;
import static java.lang.Boolean.parseBoolean;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.SECONDS;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.jboss.netty.handler.queue.BlockingReadTimeoutException;

import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.ParameterPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.Processor;
import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;



class PGConnection extends BasicContext implements Connection {



  long statementId = 0l;
  long portalId = 0l;
  int savepointId;
  private int holdability;
  boolean autoCommit = true;
  int networkTimeout;
  SQLWarning warningChain;
  List<WeakReference<PGStatement>> activeStatements;



  PGConnection(SocketAddress address, Properties settings) throws IOException {
    super(address, settings, Collections.<String, Class<?>>emptyMap());
    activeStatements = new ArrayList<>();
  }

  @Override
  public void init() throws IOException, NoticeException {

    super.init();

    applySettings(settings);
  }

  void applySettings(Properties settings) throws IOException {

    if(parseBoolean(settings.getProperty(CONNECTION_READONLY, "false"))) {
      try {
        setReadOnly(true);
      }
      catch(SQLException e) {
        throw new IOException(e);
      }
    }
  }

  protected void finalize() throws SQLException {
    close();
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

    if(isClosed())
      throw new SQLException("connection closed");
  }

  /**
   * Ensures the connection is currently in manual-commit mode
   *
   * @throws SQLException
   *          If the connection is not in manual-commit mode
   */
  void checkManualCommit() throws SQLException {

    if(autoCommit != false)
      throw new SQLException("must not be in auto-commit mode");
  }

  /**
   * Ensures the connection is currently in auto-commit mode
   *
   * @throws SQLException
   *          IF the connection is not in auto-commit mode
   */
  void checkAutoCommit() throws SQLException {

    if(autoCommit != false)
      throw new SQLException("must be in auto-commit mode");
  }

  /**
   * Ensures that a transaction is active when in manual commit mode
   *
   * @throws SQLException
   */
  void checkTransaction() throws SQLException {

    if(!autoCommit && protocol.getTransactionStatus() == Idle) {
      try {
        execQuery(getBeginText());
      }
      catch(IOException e) {
        throw new SQLException(e);
      }
      catch(NoticeException e) {
        throw makeSQLException(e.getNotice());
      }
    }

  }

  /**
   * Generates and returns the next unique statement name for this connection
   *
   * @return New unique statement name
   */
  String getNextStatementName() {
    return String.format("%016X", ++statementId);
  }

  /**
   * Generates and returns the next unique portal name for this connection
   *
   * @return New unique portal name
   */
  String getNextPortalName() {
    return String.format("%016X", ++portalId);
  }

  /**
   * Called by statements to notify the connection of their closure
   *
   * @param statement
   */
  void handleStatementClosure(PGStatement statement) {

    Iterator<WeakReference<PGStatement>> stmtRefIter = activeStatements.iterator();
    while(stmtRefIter.hasNext()) {

      PGStatement stmt = stmtRefIter.next().get();
      if(stmt == null || stmt == statement) {

        stmtRefIter.remove();
      }

    }

  }

  /**
   * Closes all active statements for this connection
   *
   * @throws SQLException
   */
  void closeStatements() throws SQLException {

    for(WeakReference<PGStatement> stmtRef : activeStatements) {

      PGStatement statement = stmtRef.get();
      if(statement != null)
        statement.internalClose();
    }
  }

  SQLText parseSQL(String sqlText) throws SQLException {
    try {
      return new SQLText(sqlText);
    }
    catch(ParseException e) {

      throw new SQLException("Error parsing SQL at position " + e.getErrorOffset());
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

    if(checkTxn) {
      checkTransaction();
    }

    //Enable network timeout
    long networkTimeoutMS = SECONDS.toMillis(networkTimeout);
    cmd.setNetworkTimeout(networkTimeoutMS);

    try {

      protocol.execute(cmd);

      if(cmd.getError() != null) {

        throw makeSQLException(cmd.getError());
      }

      return makeSQLWarningChain(cmd.getWarnings());

    }
    catch(BlockingReadTimeoutException e) {

      close();

      throw new SQLTimeoutException(e);
    }
    catch(InterruptedIOException e) {

      close();

      throw CLOSED_CONNECTION;
    }
    catch(IOException e) {

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

    if(checkTxn) {
      checkTransaction();
    }

    try {

      execQuery(sql);

    }
    catch(BlockingReadTimeoutException e) {

      close();

      throw new SQLTimeoutException(e);
    }
    catch(InterruptedIOException e) {

      close();

      throw CLOSED_CONNECTION;
    }
    catch(IOException e) {

      throw new SQLException(e);

    }
    catch(NoticeException e) {

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

    if(checkTxn) {
      checkTransaction();
    }

    try {

      return execQueryForString(sql);

    }
    catch(BlockingReadTimeoutException e) {

      close();

      throw new SQLTimeoutException(e);
    }
    catch(InterruptedIOException e) {

      close();

      throw CLOSED_CONNECTION;
    }
    catch(IOException e) {

      throw new SQLException(e);

    }
    catch(NoticeException e) {

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
  <T> T executeForResult(String sql, boolean checkTxn, Class<T> returnType, Object... params) throws SQLException {

    if(checkTxn) {
      checkTransaction();
    }

    try {

      List<Object[]> res = execQuery(sql, Object[].class, params);
      if(res.isEmpty())
        return null;

      Object[] resRow = res.get(0);
      if(resRow.length == 0)
        return null;

      return returnType.cast(resRow[0]);

    }
    catch(BlockingReadTimeoutException e) {

      close();

      throw new SQLTimeoutException(e);
    }
    catch(InterruptedIOException e) {

      close();

      throw CLOSED_CONNECTION;
    }
    catch(IOException e) {

      throw new SQLException(e);

    }
    catch(NoticeException e) {

      throw makeSQLException(e.getNotice());

    }

  }

  /**
   * Closes all statemens and shuts down the protocol
   *
   * @throws SQLException If an error occurs closing any of the statements
   */
  void internalClose() throws SQLException {

    closeStatements();

    shutdown();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {

    //Not valid if connection is closed
    if(isClosed())
      return false;

    return executeForString("SELECT '1'::char", false).equals("1");
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

    if( holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT &&
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
    if(this.autoCommit == autoCommit)
      return;

    // Commit any in-flight transaction (cannot call commit as it will start a
    // new transaction since we would still be in manual commit mode)
    if(!this.autoCommit && protocol.getTransactionStatus() != Idle) {
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

    if(protocol.getTransactionStatus() != Idle) {
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

    if( level != Connection.TRANSACTION_NONE &&
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
    if(protocol.getTransactionStatus() != Idle) {
      execute(getCommitText(), false);
    }

  }

  @Override
  public void rollback() throws SQLException {
    checkClosed();
    checkManualCommit();

    // Roll back the current transaction
    if(protocol.getTransactionStatus() != Idle) {
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

    if(!savepoint.isValid()) {
      throw new SQLException("invalid savepoint");
    }

    try {
      // Rollback to save-point (if in transaction)
      if(protocol.getTransactionStatus() != Idle) {
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

    if(!savepoint.isValid()) {
      throw new SQLException("invalid savepoint");
    }

    try {
      // Release the save-point (if in a transaction)
      if(!savepoint.getReleased() && protocol.getTransactionStatus() != Idle) {
        execute(getReleaseSavepointText((PGSavepoint) savepoint), false);
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

    final int[] parameterCount = new int[1];
    sqlText.process(new Processor() {

      @Override
      public Node process(Node node) throws SQLException {
        if(node instanceof ParameterPiece)
          parameterCount[0] += 1;
        return node;
      }

    }, true);

    PGPreparedStatement statement =
        new PGPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, statementName, sqlText.toString(), parameterCount[0]);

    activeStatements.add(new WeakReference<PGStatement>(statement));

    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();

    SQLText sqlText = parseSQL(sql);

    if(autoGeneratedKeys != RETURN_GENERATED_KEYS) {
      return prepareStatement(sql);
    }

    if(appendReturningClause(sqlText) == false) {
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

    if(appendReturningClause(sqlText, asList(columnNames)) == false) {
      throw INVALID_COMMAND_FOR_GENERATED_KEYS;
    }

    PGPreparedStatement statement = prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);

    statement.setWantsGeneratedKeys(true);

    return statement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkClosed();

    int loOid = LargeObject.creat(this, 0);

    return new PGBlob(this, loOid);
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
    if(type == null) {
      throw new SQLException("Array type not found");
    }

    return new PGArray(this, (ArrayType)type, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    checkClosed();

    Type type = getRegistry().loadType(typeName);
    if(type instanceof CompositeType == false) {
      throw new SQLException("Invalid type for struct");
    }

    return new PGStruct(this, (CompositeType)type, attributes);
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
  public Clob createClob() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return protocol.isConnected() == false;
  }

  @Override
  public void close() throws SQLException {

    // Ignore multiple closes
    if(isClosed())
      return;

    internalClose();
  }

  @Override
  public void abort(Executor executor) throws SQLException {

    getProtocol().abort(executor);

    shutdown();
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

    if(networkTimeout < 0) {
      throw new SQLException("invalid network timeout");
    }

    this.networkTimeout = networkTimeout;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if(!iface.isAssignableFrom(getClass())) {
      throw UNWRAP_ERROR;
    }

    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

}
