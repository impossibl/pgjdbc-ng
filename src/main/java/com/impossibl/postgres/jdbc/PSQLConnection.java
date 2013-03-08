package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.PSQLTextUtils.getBeginText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getCommitText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getProtocolSQLText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getReleaseSavepointText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getRollbackText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getRollbackToText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getSetSavepointText;
import static com.impossibl.postgres.jdbc.PSQLTextUtils.getSetSessionIsolationLevelText;
import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Collections.unmodifiableMap;

import java.io.IOException;
import java.net.Socket;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.ExecuteCommand;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.types.Type;



public class PSQLConnection extends BasicContext implements Connection {

	long statementId = 0l;
	long portalId = 0l;
	int savepointId;
	private int holdability;
	boolean autoCommit = true;
	int networkTimeout;

	public PSQLConnection(Socket socket, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		super(socket, settings, targetTypeMap);
	}

	public String getNextStatementName() {
		return String.format("%016X", ++statementId);
	}

	public String getNextPortalName() {
		return String.format("%016X", ++portalId);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		// TODO

	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return unmodifiableMap(targetTypeMap);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
		targetTypeMap = new HashMap<>(typeMap);
	}

	@Override
	public int getHoldability() throws SQLException {
		return holdability;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		this.holdability = holdability;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new SQLException("not supported");
	}

	private void execute(String sql) throws SQLException {

		execute(new ExecuteCommand(sql));
	}

	private void execute(Command cmd) throws SQLException {

		try {

			cmd.execute(this);

			if (cmd.getError() != null) {

				throw new SQLException(cmd.getError().message);
			}

		}
		catch (IOException e) {

			throw new SQLException(e);
		}

	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {

		if (protocol.getTransactionStatus() != Idle)
			throw new SQLException("cannot set auto-commit while transaction is active");

		this.autoCommit = autoCommit;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {

		if (autoCommit)
			throw new SQLException("cannot set isolation level when auto-commit is enabled");

		execute(getSetSessionIsolationLevelText(level));
	}

	@Override
	public void commit() throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("no transaction is active");

		execute(getCommitText());

		if (!autoCommit) {
			execute(getBeginText());
		}

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("transaction not active");

		PSQLSavepoint savepoint = new PSQLSavepoint(++savepointId);

		execute(getSetSavepointText(savepoint));

		return savepoint;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("transaction not active");

		PSQLSavepoint savepoint = new PSQLSavepoint(name);

		execute(getSetSavepointText(savepoint));

		return savepoint;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("transaction not active");

		execute(getRollbackToText((PSQLSavepoint) savepoint));

	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("transaction not active");

		execute(getReleaseSavepointText((PSQLSavepoint) savepoint));

	}

	@Override
	public void rollback() throws SQLException {

		if (protocol.getTransactionStatus() != Active)
			throw new SQLException("transaction not active");

		execute(getRollbackText());

		if (!autoCommit)
			execute(getBeginText());
	}

	@Override
	public String getCatalog() throws SQLException {
		return null;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {

		return getProtocolSQLText(sql);
	}

	@Override
	public Statement createStatement() throws SQLException {

		return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

		return createStatement(resultSetType, resultSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {

		return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

		return prepareStatement(sql, resultSetType, resultSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

		sql = nativeSQL(sql);

		String statementName = getNextStatementName();

		PrepareCommand prepare = new PrepareCommand(statementName, sql, Collections.<Type> emptyList());

		execute(prepare);

		return new PSQLStatement(this, statementName, prepare.getDescribedParameterTypes());
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
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
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO: implement
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return networkTimeout;
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		networkTimeout = milliseconds;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

}
