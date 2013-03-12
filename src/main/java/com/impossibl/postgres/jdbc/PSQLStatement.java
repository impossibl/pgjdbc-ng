package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.types.Type;



public class PSQLStatement implements PreparedStatement {

	
	
	PSQLConnection connection;
	int type;
	int concurrency;
	int holdability;
	int fetchDirection;
	String name;
	List<Type> parameterTypes;
	List<Object> parameterValues;
	List<ResultField> resultFields;
	Integer maxRows;
	Integer fetchSize;
	Integer maxFieldSize;
	BindExecCommand command;
	boolean autoClose;
	List<PSQLResultSet> activeResultSets;
	private SQLWarning warningChain;

	
	
	PSQLStatement(PSQLConnection connection, String name, List<Type> parameterTypes, List<ResultField> resultFields) {
		this.connection = connection;
		this.name = name;
		this.parameterTypes = parameterTypes;
		this.parameterValues = Arrays.asList(new Object[parameterTypes.size()]);
		this.resultFields = resultFields;
		this.activeResultSets = new ArrayList<>();
	}

	/**
	 * Ensure the connection is not closed
	 * 
	 * @throws SQLException
	 * 					If the connection is closed
	 */
	void checkClosed() throws SQLException {
		
		if(isClosed())
			throw new SQLException("closed statement");
	}
	
	/**
	 * Ensure the given parameter index is valid for this statement
	 * 
	 * @throws SQLException
	 * 					If the parameter index is out of bounds
	 */
	void checkParameterIndex(int idx) throws SQLException {
		
		if(idx < 1 && idx > parameterValues.size())
			throw new SQLException("parameter index out of bounds");
	}

	/**
	 * Disposes of the named server object
	 * 
	 * @param objectType
	 * 					Type of object to dispose of
	 * @param objectName
	 * 					Name of the object to dispose of
	 * @throws SQLException
	 * 					If an error occurs during disposal
	 */
	void dispose(ServerObjectType objectType, String objectName) throws SQLException {
		
		if(objectName == null)
			return;
	
		CloseCommand close = connection.getProtocol().createClose(objectType, objectName);
		
		connection.execute(close);		
	}
	
	/**
	 * Closes all active result sets for this statement
	 * 
	 * @throws SQLException
	 * 					If an error occurs closing a result set
	 */
	void closeResultSets() throws SQLException {
		
		for(PSQLResultSet rs : activeResultSets) {
			
			rs.internalClose();
			
		}
		
	}
	
	/**
	 * Called by result sets to notify the statement of their closure. Removes
	 * the result set from the active set of result sets. If auto-close is
	 * enabled this closes the statement when the last result set is closed.
	 * 
	 * @param resultSet
	 * 					The result set that is closing
	 * @throws SQLException
	 * 					If an error occurs closing a result set
	 */
	void handleResultSetClosure(PSQLResultSet resultSet) throws SQLException {
		
		activeResultSets.remove(resultSet);
		
		if(autoClose && activeResultSets.isEmpty()) {
			
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
	 * 					If an error occurs closing result sets or 
	 */
	void internalClose() throws SQLException {

		closeResultSets();
		
		dispose(Statement, name);
		
		connection = null;
		command = null;
		parameterTypes = null;
		parameterValues = null;
		resultFields = null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		checkClosed();
		
		return connection;
	}

	@Override
	public int getResultSetType() throws SQLException {
		checkClosed();
		
		return type;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		checkClosed();
		
		return concurrency;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		checkClosed();
		
		return holdability;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
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
		
		if(max < 0)
			throw new SQLException("illegal argument");
		
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

		if(max < 0)
			throw new SQLException("illegal argument");
		
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
		
		if (direction != ResultSet.FETCH_FORWARD ||
				direction != ResultSet.FETCH_REVERSE ||
				direction != ResultSet.FETCH_UNKNOWN)
			throw new SQLException("illegal argument");
			
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
		
		if(rows < 0)
			throw new SQLException("illegal argument");
		
		fetchSize = rows;
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkClosed();
		
		throw new UnsupportedOperationException();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub

	}

	@Override
	public boolean execute() throws SQLException {
		
		closeResultSets();

		String portalName = null;
		
		if (needsNamedPortal()) {
			portalName = connection.getNextPortalName();
		}

		command = connection.getProtocol().createBindExec(portalName, name, parameterTypes, parameterValues, resultFields, Object[].class);

		if(fetchSize != null)
			command.setMaxRows(fetchSize);
		
		if(maxFieldSize != null)
			command.setMaxFieldLength(maxFieldSize);

		warningChain = connection.execute(command);

		return !command.getResultFields().isEmpty();
		
	}

	@Override
	public ResultSet executeQuery() throws SQLException {

		execute();

		return getResultSet();
	}

	@Override
	public int executeUpdate() throws SQLException {

		execute();

		return getUpdateCount();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
	}

	@Override
	public void clearBatch() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
	}

	@Override
	public int[] executeBatch() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addBatch() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkClosed();

		if (command.getResultFields().isEmpty()) {
			throw new SQLException("no result set available");
		}

		PSQLResultSet rs = new PSQLResultSet(this, command);
		this.activeResultSets.add(rs);
		return rs;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		checkClosed();

		if (command.getResultRowsAffected() == null) {
			throw new SQLException("no update count available");
		}

		return (int) (long) command.getResultRowsAffected();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cancel() throws SQLException {
		checkClosed();
		// TODO Auto-generated method stub
	}

	@Override
	public boolean isClosed() throws SQLException {
		return connection == null;
	}

	@Override
	public void close() throws SQLException {

		// Ignore multiple closes
		if(isClosed())
			return;

		connection.handleStatementClosure(this);
		
		internalClose();
	}
	
	@Override
	public void clearParameters() throws SQLException {
		checkClosed();

		for (int c = 0; c < parameterValues.size(); ++c) {
		
			parameterValues.set(c, null);
		
		}
		
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, null);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

		setBinaryStream(parameterIndex, x, (long) -1);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		
		setBinaryStream(parameterIndex, x, (long) length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			ByteStreams.copy(x, out);
		}
		catch(IOException e) {
			throw new SQLException(e);
		}

		parameterValues.set(parameterIndex - 1, out.toByteArray());
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

		InputStreamReader reader = new InputStreamReader(x, UTF_8);
		
		setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

		setAsciiStream(parameterIndex, x, (long) -1);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

		setAsciiStream(parameterIndex, x, (long) length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

		InputStreamReader reader = new InputStreamReader(x, US_ASCII);
		
		setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

		setCharacterStream(parameterIndex, reader, (long) -1);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		
		setCharacterStream(parameterIndex, reader, (long) length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		
		StringWriter writer = new StringWriter();
		try {
			CharStreams.copy(reader, writer);
		}
		catch(IOException e) {
			throw new SQLException(e);
		}
		
		parameterValues.set(parameterIndex - 1, writer.toString());
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);
		
		parameterValues.set(parameterIndex - 1, x);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		checkClosed();
		throw new UnsupportedOperationException();
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
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

}
