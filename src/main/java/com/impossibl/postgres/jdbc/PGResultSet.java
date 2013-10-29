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

import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Type;
import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_RESULT_SET;
import static com.impossibl.postgres.jdbc.Exceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.CURSOR_NOT_SCROLLABLE;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;
import static com.impossibl.postgres.jdbc.Exceptions.INVALID_COLUMN_NAME;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.ROW_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBigDecimal;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBlob;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBoolean;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToByte;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBytes;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDate;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDouble;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToFloat;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToInt;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToLong;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToShort;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToString;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTime;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTimestamp;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToURL;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToXML;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapGetType;
import static com.impossibl.postgres.protocol.QueryCommand.Status.Completed;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
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
import java.util.TimeZone;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

class PGResultSet implements ResultSet {



  PGStatement statement;
  int type;
  int concurrency;
  int holdability;
  Integer fetchDir;
  Integer fetchSize;
  int resultsIndexOffset;
  int currentRowIndex;
  QueryCommand command;
  SQLWarning warningChain;
  List<ResultField> resultFields;
  List<Object[]> results;
  Boolean nullFlag;
  Map<String, Class<?>> typeMap;



  PGResultSet(PGStatement statement, int concurrency, QueryCommand command, List<ResultField> resultFields, List<?> results) throws SQLException {
    this(statement, command.getStatus() == Completed ? TYPE_SCROLL_INSENSITIVE : TYPE_FORWARD_ONLY, concurrency, resultFields, results);
    this.command = command;
  }

  PGResultSet(PGStatement statement, int type, int concurrency, List<ResultField> resultFields, List<?> results) throws SQLException {
    this(statement, type, concurrency, resultFields, results, statement.connection.getTypeMap());
  }

  @SuppressWarnings("unchecked")
  PGResultSet(PGStatement statement, int type, int concurrency, List<ResultField> resultFields, List<?> results, Map<String, Class<?>> typeMap) {
    this.type = type;
    this.concurrency = concurrency;
    this.statement = statement;
    this.fetchDir = FETCH_FORWARD;
    this.fetchSize = statement.fetchSize;
    this.resultsIndexOffset = 0;
    this.currentRowIndex = -1;
    this.resultFields = resultFields;
    this.results = (List<Object[]>)results;
    this.typeMap = typeMap;
  }

  /**
   * Ensure the result set is not closed
   *
   * @throws SQLException If the connection is closed
   */
  void checkClosed() throws SQLException {

    if (isClosed())
      throw CLOSED_RESULT_SET;

  }

  /**
   * Ensure the columnIndex is in a valid range for this result set
   *
   * @param columnIndex Column index to range check
   * @throws SQLException If the provided index is out of the range
   */
  void checkColumnIndex(int columnIndex) throws SQLException {

    if (columnIndex < 1 || columnIndex > resultFields.size())
      throw COLUMN_INDEX_OUT_OF_BOUNDS;

  }

  /**
   * Is the currentRowIndex pointer pointing to a valid row?
   *
   * @return True if the currentRowIndex is a valid row, false otherwise
   */
  boolean isValidRow() {
    return currentRowIndex >= 0 && currentRowIndex < results.size();
  }

  /**
   * Ensure the current row index is in a valid range for this result set
   *
   * @throws SQLException If the current row index is out of the range
   */
  void checkRow() throws SQLException {

    if (!isValidRow())
      throw ROW_INDEX_OUT_OF_BOUNDS;

  }

  /**
   * Ensure the result set is scrollable
   *
   * @throws SQLException If the connection is closed
   */
  void checkScroll() throws SQLException {

    if (type == TYPE_FORWARD_ONLY)
      throw CURSOR_NOT_SCROLLABLE;

  }

  /**
   * Retrieves the column using the correct index and properly sets the
   * null flag for subsequent operations
   *
   * @param columnIndex Column index to retrieve
   * @return Column value as Object
   */
  Object get(int columnIndex) {
    Object val = results.get(currentRowIndex)[columnIndex - 1];
    nullFlag = val == null;
    return val;
  }

  Type getType(int columnIndex) {
    return resultFields.get(columnIndex - 1).typeRef.get();
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
    return fetchDir != null ? fetchDir : 0;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    if (direction != FETCH_FORWARD) {
      checkScroll();
    }
    fetchDir = direction;
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
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();

    return !results.isEmpty() && resultsIndexOffset == 0 && currentRowIndex == -1;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();

    return !results.isEmpty() && command.getStatus() == Completed && currentRowIndex == results.size();
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();

    return !results.isEmpty() && resultsIndexOffset == 0 && currentRowIndex == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();

    return !results.isEmpty() && command.getStatus() == Completed && currentRowIndex == results.size() - 1;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();
    checkScroll();

    currentRowIndex = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClosed();
    checkScroll();

    currentRowIndex = results.size();
  }

  @Override
  public boolean first() throws SQLException {
    checkClosed();
    checkScroll();

    return absolute(1);
  }

  @Override
  public boolean last() throws SQLException {
    checkClosed();
    checkScroll();

    return absolute(-1);
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();

    if (!isValidRow())
      return 0;

    return resultsIndexOffset + currentRowIndex + 1;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkClosed();
    checkScroll();

    if (row < 0) {
      row = results.size() + 1 + row;
    }

    currentRowIndex = max(-1, min(results.size(), row - 1));
    return isValidRow();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClosed();
    checkScroll();

    currentRowIndex = max(-1, min(results.size(), currentRowIndex + rows));
    return isValidRow();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (min(++currentRowIndex, results.size()) == results.size()) {

      if (command != null && command.getStatus() != Completed) {

        if (fetchSize != null)
          command.setMaxRows(fetchSize);

        warningChain = statement.connection.execute(command, true);

        List<QueryCommand.ResultBatch> resultBatches = command.getResultBatches();
        if (resultBatches.size() != 1) {
          throw new SQLException("Invalid result data");
        }

        QueryCommand.ResultBatch resultBatch = resultBatches.get(0);

        resultFields = resultBatch.fields;
        results = (List<Object[]>) resultBatch.results;

        resultsIndexOffset = currentRowIndex;
        currentRowIndex = -1;

        return next();
      }

    }

    return isValidRow();
  }

  @Override
  public boolean previous() throws SQLException {
    checkClosed();
    checkScroll();

    return relative(-1);
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void insertRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void deleteRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void refreshRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return statement == null;
  }

  @Override
  public void close() throws SQLException {

    // Ignore multiple closes
    if (isClosed())
      return;

    // Notify statement of our closure
    statement.handleResultSetClosure(this);

    internalClose();
  }

  void internalClose() throws SQLException {

    //Release resources

    if (command != null) {
      statement.dispose(command);
    }

    statement = null;
    command = null;
    results = null;
    resultFields = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();
    return new PGResultSetMetaData(statement.connection, resultFields, typeMap);
  }

  @Override
  public boolean wasNull() throws SQLException {
    checkClosed();

    if (nullFlag == null)
      throw new SQLException("no column fetched");

    return nullFlag;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToString(get(columnIndex), statement.connection);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToBoolean(get(columnIndex));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToByte(get(columnIndex));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToShort(get(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToInt(get(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToLong(get(columnIndex));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToFloat(get(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToDouble(get(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {

    BigDecimal val = coerceToBigDecimal(columnIndex);
    if (val == null) {
      return null;
    }

    return val.setScale(scale, HALF_UP);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToBigDecimal(get(columnIndex));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToBytes(get(columnIndex), getType(columnIndex), statement.connection);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {

    return getDate(columnIndex, Calendar.getInstance());
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {

    return getTime(columnIndex, Calendar.getInstance());
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {

    return getTimestamp(columnIndex, Calendar.getInstance());
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    TimeZone zone = cal.getTimeZone();

    return coerceToDate(get(columnIndex), zone, statement.connection);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    TimeZone zone = cal.getTimeZone();

    return coerceToTime(get(columnIndex), zone, statement.connection);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    TimeZone zone = cal.getTimeZone();

    return coerceToTimestamp(get(columnIndex), zone, statement.connection);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    Object value = get(columnIndex);
    if (value == null)
      return null;

    Type type = getType(columnIndex);

    if (!(type instanceof ArrayType)) {
      throw SQLTypeUtils.createCoercionException(value.getClass(), Array.class);
    }

    return new PGArray(statement.connection, (ArrayType)type, (Object[])value);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return coerceToURL(get(columnIndex));
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {

    String data = getString(columnIndex);
    if (data == null)
      return null;

    return new StringReader(data);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {

    String data = getString(columnIndex);
    if (data == null)
      return null;

    return new ByteArrayInputStream(data.getBytes(US_ASCII));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {

    String data = getString(columnIndex);
    if (data == null)
      return null;

    return new ByteArrayInputStream(data.getBytes(UTF_8));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {

    byte[] data = getBytes(columnIndex);
    if (data == null)
      return null;

    return new ByteArrayInputStream(data);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {

    return coerceToBlob(get(columnIndex), statement.connection);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {

    return coerceToXML(get(columnIndex), statement.connection);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getObject(columnIndex, typeMap);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    Type type = getType(columnIndex);

    Class<?> targetType = mapGetType(type, map, statement.connection);

    return coerce(get(columnIndex), type, targetType, map, statement.connection);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return type.cast(coerce(get(columnIndex), getType(columnIndex), type, typeMap, statement.connection));
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    checkClosed();

    for (int c = 0; c < resultFields.size(); ++c) {

      if (resultFields.get(c).name.equalsIgnoreCase(columnLabel))
        return c + 1;
    }

    throw INVALID_COLUMN_NAME;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return getObject(findColumn(columnLabel), map);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return getRef(findColumn(columnLabel));
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getBlob(findColumn(columnLabel));
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getClob(findColumn(columnLabel));
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getArray(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(findColumn(columnLabel));
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return getRowId(findColumn(columnLabel));
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return getNClob(findColumn(columnLabel));
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return getSQLXML(findColumn(columnLabel));
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getNString(findColumn(columnLabel));
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getNCharacterStream(findColumn(columnLabel));
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw NOT_SUPPORTED;
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

}
