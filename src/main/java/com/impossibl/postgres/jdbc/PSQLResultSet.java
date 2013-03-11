package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.protocol.ServerObject.Portal;
import static com.impossibl.postgres.protocol.v30.BindExecCommandImpl.Status.Completed;
import static java.lang.Math.min;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.ResultField;



public class PSQLResultSet implements ResultSet {

	
	
	PSQLStatement statement;
	int type;
	int concurrency;
	int holdability;
	Integer fetchSize;
	int currentRow;
	BindExecCommand command;
	List<ResultField> resultFields;
	List<Object[]> results;
	Boolean nullFlag;

	
	
	public PSQLResultSet(PSQLStatement statement, BindExecCommand command) {
		super();
		this.statement = statement;
		this.fetchSize = statement.fetchSize;
		this.currentRow = -1;
		this.command = command;
		this.resultFields = command.getResultFields();
		this.results = command.getResults(Object[].class);
	}
	
	void checkClosed() throws SQLException {
		
		if(isClosed())
			throw new SQLException("closed result set");
		
	}
	
	void checkColumnIndex(int columnIndex) throws SQLException {
		
		if(columnIndex < 1 || columnIndex > resultFields.size())
			throw new SQLException("column index out of bounds");
		
	}
	
	Object get(int columnIndex) throws SQLException {
		Object val = results.get(currentRow)[columnIndex-1];
		nullFlag = val == null;
		return val;
	}

	@Override
	public Statement getStatement() throws SQLException {
		checkClosed();
		
		return statement;
	}

	@Override
	public int getType() throws SQLException {
		checkClosed();
		
		return type;
	}

	@Override
	public int getConcurrency() throws SQLException {
		checkClosed();
		
		return concurrency;
	}

	@Override
	public int getHoldability() throws SQLException {
		checkClosed();
		
		return holdability;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();
		
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		checkClosed();
		
		// TODO Auto-generated method stub
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
	public boolean next() throws SQLException {
		checkClosed();
		
		if (min(++currentRow, results.size()) == results.size()) {

			if(command.getStatus() != Completed) {
				
				if(fetchSize != null)
					command.setMaxRows(fetchSize);
				
				statement.connection.execute(command);
				
				resultFields = command.getResultFields();
				results = (List<Object[]>) command.getResults(Object[].class);
				currentRow = -1;
				
				return next();				
			}
			
			return false;
		}

		return true;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkClosed();

		return currentRow == -1;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkClosed();

		return currentRow == results.size();
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkClosed();

		return currentRow == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		checkClosed();

		return currentRow == results.size() - 1;
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();

		currentRow = -1;
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();

		currentRow = results.size();
	}

	@Override
	public boolean first() throws SQLException {
		checkClosed();

		currentRow = 0;
		return true;
	}

	@Override
	public boolean last() throws SQLException {
		checkClosed();

		currentRow = results.size() - 1;
		return true;
	}

	@Override
	public int getRow() throws SQLException {
		checkClosed();

		return currentRow;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		checkClosed();

		currentRow = row;
		return true;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		checkClosed();

		currentRow += rows;
		return true;
	}

	@Override
	public boolean previous() throws SQLException {
		checkClosed();

		currentRow--;
		return true;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void insertRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void updateRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void deleteRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void refreshRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
	}

	@Override
	public boolean isClosed() throws SQLException {
		return statement == null;
	}

	@Override
	public void close() throws SQLException {
		
		// Ignore multiple closes
		if(isClosed())
			return;
		
		// Notify statement of our closure
		statement.notifyClosure(this);

		internalClose();		
	}
	
	void internalClose() throws SQLException {

		//Dispose of the portal if we are using one
		
		if(command != null) {
			
			statement.dispose(Portal, command.getPortalName());
		}
		
		//Release resources
		statement = null;
		command = null;
		results = null;
		resultFields = null;
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();

		if(nullFlag == null)
			throw new SQLException("no column fetched");
		
		return nullFlag;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
				
		return (String) get(columnIndex);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Boolean) get(columnIndex);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Byte) get(columnIndex);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Short) get(columnIndex);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Integer) get(columnIndex);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Long) get(columnIndex);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Float) get(columnIndex);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Double) get(columnIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (BigDecimal) get(columnIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (BigDecimal) get(columnIndex);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (byte[]) get(columnIndex);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Date) get(columnIndex);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Time) get(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Timestamp) get(columnIndex);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Date) get(columnIndex);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Time) get(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return (Timestamp) get(columnIndex);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return new ByteArrayInputStream((byte[]) get(columnIndex));
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);

		return get(columnIndex);
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);

		//TODO respect type-map
		
		return get(columnIndex);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		
		return type.cast(get(columnIndex));
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		checkClosed();		
		throw new UnsupportedOperationException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		checkClosed();		
		throw new UnsupportedOperationException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		checkClosed();		
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		checkClosed();		
		throw new UnsupportedOperationException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		checkClosed();
		
		return (String) get(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		checkClosed();
		
		return (Boolean) get(findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		checkClosed();
		
		return (Byte) get(findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		checkClosed();
		
		return (Short) get(findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		checkClosed();

		return (Integer) get(findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		checkClosed();

		return (Long) get(findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		checkClosed();

		return (Float) get(findColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		checkClosed();

		return (Double) get(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		checkClosed();

		return (BigDecimal) get(findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		checkClosed();

		return (byte[]) get(findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		checkClosed();

		return (Date) get(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		checkClosed();

		return (Time) get(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		checkClosed();

		return (Timestamp) get(findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		checkClosed();

		return new ByteArrayInputStream((byte[]) get(findColumn(columnLabel)));
	}

	@Override
	public String getCursorName() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		checkClosed();

		return get(findColumn(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {

		for (int c = 0; c < resultFields.size(); ++c) {

			if (resultFields.get(c).name.equals(columnLabel))
				return c;
		}

		throw new SQLException("invalid column");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

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
