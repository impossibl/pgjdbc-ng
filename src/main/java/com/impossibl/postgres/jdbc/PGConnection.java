package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLWarningChain;
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
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;

import java.io.IOException;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.Type;



class PGConnection extends BasicContext implements Connection {

	
	
	long statementId = 0l;
	long portalId = 0l;
	int savepointId;
	private int holdability;
	boolean autoCommit = true;
	boolean readOnly = false;
	int networkTimeout;
	SQLWarning warningChain;
	List<WeakReference<PGStatement>> activeStatements;

	
	
	PGConnection(SocketAddress address, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		super(address, settings, targetTypeMap);
		activeStatements = new ArrayList<>();
	}
	
	protected void finalize() throws SQLException {
		close();
	}

	/**
	 * Ensure the connection is not closed
	 * 
	 * @throws SQLException
	 * 					If the connection is closed
	 */
	void checkClosed() throws SQLException {

		if(isClosed())
			throw new SQLException("connection closed");
	}

	/**
	 * Ensures the connection is currently in manual-commit mode
	 * 
	 * @throws SQLException
	 * 					If the connection is not in manual-commit mode
	 */
	void checkManualCommit() throws SQLException {

		if(autoCommit != false)
			throw new SQLException("must not be in auto-commit mode");
	}

	/**
	 * Ensures the connection is currently in auto-commit mode
	 * 
	 * @throws SQLException
	 * 					IF the connection is not in auto-commit mode
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
	SQLWarning execute(Command cmd) throws SQLException {

		checkTransaction();
		
		try {
			
			protocol.execute(cmd);

			if(cmd.getError() != null) {

				throw makeSQLException(cmd.getError());
			}

			return makeSQLWarningChain(cmd.getWarnings());

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
	void execute(String sql) throws SQLException {

		checkTransaction();
		
		try {

			execQuery(sql);

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
	String executeForString(String sql) throws SQLException {

		checkTransaction();
		
		try {

			return execQueryForString(sql);

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
	<T> T executeForResult(String sql, Class<T> returnType, Object... params) throws SQLException {

		checkTransaction();
		
		try {

			List<Object[]> res = execQuery(sql, Object[].class, params);
			if(res.isEmpty())
				return null;
			
			Object[] resRow = res.get(0);
			if(resRow.length == 0)
				return null;
			
			return returnType.cast(resRow[0]);

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
		
		return executeForString("SELECT '1'::char").equals("1");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkClosed();
		
		return unmodifiableMap(targetTypeMap);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
		checkClosed();
		
		targetTypeMap = new HashMap<>(typeMap);
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

		if(protocol.getTransactionStatus() != Idle) {
			throw new SQLException("cannot set read only during a transaction");
		}

		execute(getSetSessionReadabilityText(readOnly));
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		checkClosed();

		String isolLevel = executeForString(getGetSessionIsolationLevelText());

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

		execute(getSetSessionIsolationLevelText(level));
	}

	@Override
	public void commit() throws SQLException {
		checkClosed();
		checkManualCommit();

		// Commit the current transaction
		if(protocol.getTransactionStatus() != Idle) {
			execute(getCommitText());
		}

	}

	@Override
	public void rollback() throws SQLException {
		checkClosed();
		checkManualCommit();

		// Roll back the current transaction
		if(protocol.getTransactionStatus() != Idle) {
			execute(getRollbackText());
		}

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		checkClosed();
		checkManualCommit();

		// Allocate new save-point name & wrapper
		PGSavepoint savepoint = new PGSavepoint(++savepointId);

		// Mark save-point (will auto start txn if needed)
		execute(getSetSavepointText(savepoint));

		return savepoint;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		checkClosed();
		checkManualCommit();

		// Allocate new save-point wrapper
		PGSavepoint savepoint = new PGSavepoint(name);

		// Mark save-point (will auto start txn if needed)
		execute(getSetSavepointText(savepoint));

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

		// Use up the savepoint
		savepoint.invalidate();

		// Rollback to save-point (if in transaction)
		if(protocol.getTransactionStatus() != Idle) {
			execute(getRollbackToText(savepoint));
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

		// Use up the save-point
		savepoint.invalidate();

		// Release the save-point (if in a transaction)
		if(protocol.getTransactionStatus() != Idle) {
			execute(getReleaseSavepointText((PGSavepoint) savepoint));
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

		SQLText sqlText = new SQLText(sql);
		
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
		
		SQLText sqlText = new SQLText(sql);
		
		return prepareStatement(sqlText, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	
	public PGPreparedStatement prepareStatement(SQLText sqlText, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		
		SQLTextEscapes.processEscapes(sqlText, this);
		
		String statementName = getNextStatementName();

		PrepareCommand prepare = protocol.createPrepare(statementName, sqlText.toString(), Collections.<Type> emptyList());

		warningChain = execute(prepare);

		PGPreparedStatement statement =
				new PGPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability,
						statementName, prepare.getDescribedParameterTypes(), prepare.getDescribedResultFields());
		
		activeStatements.add(new WeakReference<PGStatement>(statement));
		
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		checkClosed();
		
		SQLText sqlText = new SQLText(sql);
		
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
		
		SQLText sqlText = new SQLText(sql);
		
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
	public Clob createClob() throws SQLException {
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
	public NClob createNClob() throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
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
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return protocol == null;
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
		throw NOT_IMPLEMENTED;
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
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		checkClosed();
		networkTimeout = milliseconds;
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
