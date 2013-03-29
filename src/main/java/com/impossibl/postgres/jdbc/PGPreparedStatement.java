package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_ALLOWED_ON_PREP_STMT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
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
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.types.Type;



class PGPreparedStatement extends PGStatement implements PreparedStatement {

	
	
	List<Type> parameterTypes;
	List<Object> parameterValues;
	boolean wantsGeneratedKeys;
	
	
	
	PGPreparedStatement(PGConnection connection, int type, int concurrency, int holdability, String name, List<Type> parameterTypes, List<ResultField> resultFields) {
		super(connection, type, concurrency, holdability, name, resultFields);
		this.parameterTypes = parameterTypes;
		this.parameterValues = Arrays.asList(new Object[parameterTypes.size()]);
	}

	public boolean getWantsGeneratedKeys() {
		return wantsGeneratedKeys;
	}

	public void setWantsGeneratedKeys(boolean wantsGeneratedKeys) {
		this.wantsGeneratedKeys = wantsGeneratedKeys;
	}

	/**
	 * Ensure the given parameter index is valid for this statement
	 * 
	 * @throws SQLException
	 * 					If the parameter index is out of bounds
	 */
	void checkParameterIndex(int idx) throws SQLException {
		
		if(idx < 1 || idx > parameterValues.size())
			throw PARAMETER_INDEX_OUT_OF_BOUNDS;
	}
	
	void set(int parameterIdx, Object val) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIdx);
		
		parameterIdx -= 1;
		
		Type paramType = parameterTypes.get(parameterIdx);
		
		parameterValues.set(parameterIdx, coerce(val, paramType, connection.getTypeMap(), connection));
	}

	void internalClose() throws SQLException {

		super.internalClose();
		
		parameterTypes = null;
		parameterValues = null;
	}

	@Override
	public boolean execute() throws SQLException {
		
		boolean res = super.executeStatement(name, parameterTypes, parameterValues);
		
		if(wantsGeneratedKeys) {
			generatedKeysResultSet = getResultSet();
		}

		return res;
	}

	@Override
	public PGResultSet executeQuery() throws SQLException {

		execute();

		return getResultSet();
	}

	@Override
	public int executeUpdate() throws SQLException {

		execute();

		if(wantsGeneratedKeys) {
			generatedKeysResultSet = getResultSet();
		}

		return getUpdateCount();
	}

	@Override
	public void clearBatch() throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public int[] executeBatch() throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void addBatch() throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void clearParameters() throws SQLException {
		checkClosed();

		for (int c = 0; c < parameterValues.size(); ++c) {
		
			parameterValues.set(c, null);
		
		}
		
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		checkClosed();
		
		return new PGParameterMetaData(parameterTypes);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		
		return new PGResultSetMetaData(connection, resultFields);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		set(parameterIndex, null);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {		
		set(parameterIndex, x);
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
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			ByteStreams.copy(x, out);
		}
		catch(IOException e) {
			throw new SQLException(e);
		}

		set(parameterIndex, out.toByteArray());
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

		StringWriter writer = new StringWriter();
		try {
			CharStreams.copy(reader, writer);
		}
		catch(IOException e) {
			throw new SQLException(e);
		}
		
		set(parameterIndex, writer.toString());
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {		
		set(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		checkClosed();
		
		setObject(parameterIndex, x, targetSqlType, 0);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		checkClosed();
		checkParameterIndex(parameterIndex);

		if(SQLTypeMetaData.getSQLType(parameterTypes.get(parameterIndex-1)) != targetSqlType) {
			throw new SQLException("Invalid target SQL type");
		}
		
		set(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		set(parameterIndex, x); 
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		setBlob(parameterIndex, ByteStreams.limit(inputStream, length));
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		
		Blob blob = connection.createBlob();
		
		try {
			ByteStreams.copy(inputStream, blob.setBinaryStream(0));
		}
		catch(IOException e) {
			throw new SQLException(e);
		}

		set(parameterIndex, blob);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		set(parameterIndex, null);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		set(parameterIndex, x);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		checkClosed();
		throw NOT_IMPLEMENTED;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw NOT_ALLOWED_ON_PREP_STMT;
	}

}
