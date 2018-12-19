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
import com.impossibl.postgres.protocol.FieldBuffersRowData;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.protocol.UpdatableRowData;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.system.TypeMapContext;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_RESULT_SET;
import static com.impossibl.postgres.jdbc.Exceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.CURSOR_NOT_SCROLLABLE;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;
import static com.impossibl.postgres.jdbc.Exceptions.INVALID_COLUMN_NAME;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.ROW_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.RS_NOT_UPDATABLE;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.jdbc.Query.Status.Completed;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapBlob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapClob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapObject;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapRowId;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.buffer.ByteBuf;

import static io.netty.util.ReferenceCountUtil.release;


class PGResultSet implements ResultSet {

  /**
   * Cleans up server resources in the event of leaking resultset
   *
   * @author kdubb
   *
   */
  private static class Cleanup implements CleanupRunnable {

    PGStatement statement;
    Query query;
    StackTraceElement[] allocationStackTrace;

    private Cleanup(PGStatement statement, Query query) {
      this.statement = statement;
      this.query = query;
      this.allocationStackTrace = new Exception().getStackTrace();
    }

    @Override
    public String getKind() {
      return "result-set";
    }

    @Override
    public StackTraceElement[] getAllocationStackTrace() {
      return allocationStackTrace;
    }

    @Override
    public void run() {

      try {
        query.dispose(statement.connection);
      }
      catch (SQLException e) {
        //Ignore...
      }

      try {
        statement.handleResultSetClosure(null);
      }
      catch (SQLException e) {
        //Ignore...
      }

      statement = null;
    }

  }


  PGStatement statement;
  private Scroller scroller;
  private int fetchDirection;
  private Integer fetchSize;
  private SQLWarning warningChain;
  private Boolean nullFlag;
  private final SettingsContext context;
  private final Housekeeper.Ref housekeeper;
  private final Object cleanupKey;

  private static final ThreadLocal<TypeMapContext> TYPE_MAP_CONTEXTS = ThreadLocal.withInitial(TypeMapContext::new);

  PGResultSet(PGStatement statement, Query query, ResultField[] resultFields, RowDataSet results) throws SQLException {
    this(statement, query, null);
    this.scroller = new QueryScroller(this, query, resultFields, results);

    if (statement.fetchDirection != ResultSet.FETCH_FORWARD) {
      if (scroller.getType() == ResultSet.TYPE_FORWARD_ONLY)
        throw CURSOR_NOT_SCROLLABLE;
    }
  }

  PGResultSet(PGStatement statement, ResultField[] resultFields, RowDataSet results, boolean releaseResults, Map<String, Class<?>> typeMap) throws SQLException {
    this(statement, null, typeMap);
    this.scroller = new ListScroller(resultFields, results, releaseResults);

    if (statement.fetchDirection != ResultSet.FETCH_FORWARD) {
      if (scroller.getType() == ResultSet.TYPE_FORWARD_ONLY)
        throw CURSOR_NOT_SCROLLABLE;
    }
  }

  PGResultSet(PGStatement statement, String cursorName, int type, int holdability, ResultField[] resultFields) throws SQLException {
    this(statement, null, null);
    this.scroller = new CursorScroller(this, cursorName, type, holdability, resultFields);

    if (statement.fetchDirection != ResultSet.FETCH_FORWARD) {
      if (scroller.getType() == ResultSet.TYPE_FORWARD_ONLY)
        throw CURSOR_NOT_SCROLLABLE;
    }
  }

  private PGResultSet(PGStatement statement, Query query, Map<String, Class<?>> typeMap) {
    this.statement = statement;
    this.fetchDirection = statement.fetchDirection;
    this.fetchSize = statement.fetchSize;

    this.context = new SettingsContext(statement.connection, typeMap);
    updateMaxFieldSize(statement.maxFieldSize);

    this.housekeeper = statement.housekeeper;
    if (this.housekeeper != null)
      this.cleanupKey = housekeeper.add(this, new Cleanup(statement, query));
    else
      this.cleanupKey = null;
  }

  void updateMaxFieldSize(Integer maxFieldSize) {
    this.context.setSetting(Settings.FIELD_VARYING_LENGTH_MAX, maxFieldSize);
  }

  /**
   * Ensure the result set is not closed
   *
   * @throws SQLException
   *           If the connection is closed
   */
  void checkClosed() throws SQLException {

    if (isClosed())
      throw CLOSED_RESULT_SET;

  }

  /**
   * Ensure the columnIndex is in a valid range for this result set
   *
   * @param columnIndex
   *          Column index to range check
   * @throws SQLException
   *           If the provided index is out of the range
   */
  private void checkColumnIndex(int columnIndex) throws SQLException {

    if (columnIndex < 1 || columnIndex > scroller.getResultFields().length)
      throw COLUMN_INDEX_OUT_OF_BOUNDS;

  }

  /**
   * Ensure the current row index is in a valid range for this result set
   *
   * @throws SQLException
   *           If the current row index is out of the range
   */
  private void checkRow() throws SQLException {

    if (!scroller.isValidRow())
      throw ROW_INDEX_OUT_OF_BOUNDS;

  }

  /**
   * Ensure the result set is updatable
   *
   * @throws SQLException
   *           If the result set is not updatable
   */
  private void checkUpdatable() throws SQLException {

    if (scroller.getConcurrency() == CONCUR_READ_ONLY)
      throw RS_NOT_UPDATABLE;
  }

  /**
   * Ensure the result set is ready to be updated
   *
   * @throws SQLException
   *           If the result set is not updatable
   */
  private void checkUpdate() throws SQLException {
    checkUpdatable();
  }

  /**
   * Retrieves the column using the correct index and properly sets the null
   * flag for subsequent operations
   *
   * @param columnIndex
   *          Column index to retrieve
   * @return Column value as Object
   */
  private <R> R getVal(int columnIndex, Context context, Class<R> targetClass, Object targetContext) throws PGSQLSimpleException {
    return targetClass.cast(getObj(columnIndex, context, targetClass, targetContext));
  }

  private Object getObj(int columnIndex, Context context, Class<?> targetClass, Object targetContext) throws PGSQLSimpleException {

    Object val;
    try {
      val = scroller.getRowField(columnIndex - 1, context, targetClass, targetContext);
    }
    catch (IOException e) {
      throw new PGSQLSimpleException("Error decoding column", e);
    }
    nullFlag = val == null;
    return val;
  }

  void set(int columnIndex, Object source, Object sourceContext) throws SQLException {
    checkClosed();
    checkColumnIndex(columnIndex);

    UpdatableRowData rowData = scroller.getUpdatableRowData();
    if (rowData == null) {
      throw RS_NOT_UPDATABLE;
    }

    try {
      ResultField field = scroller.getResultFields()[columnIndex - 1];
      rowData.updateField(columnIndex - 1, field, context, source, sourceContext);
    }
    catch (IOException e) {
      throw new PGSQLSimpleException("Error decoding column", e);
    }

  }

  ResultField[] getResultFields() {
    return scroller.getResultFields();
  }

  @Override
  public Statement getStatement() throws SQLException {
    checkClosed();

    return statement;
  }

  @Override
  public int getType() throws SQLException {
    checkClosed();

    //noinspection MagicConstant
    return scroller.getType();
  }

  @Override
  public int getConcurrency() throws SQLException {
    checkClosed();

    return scroller.getConcurrency();
  }

  @Override
  public int getHoldability() throws SQLException {
    checkClosed();

    //noinspection MagicConstant
    return scroller.getHoldability();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();
    return fetchDirection;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      if (scroller.getType() == ResultSet.TYPE_FORWARD_ONLY)
        throw CURSOR_NOT_SCROLLABLE;
    }
    fetchDirection = direction;
  }

  Integer fetchSize() {
    return fetchSize;
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

    return scroller.isBeforeFirst();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();

    return scroller.isAfterLast();
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();

    return scroller.isFirst();
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();

    return scroller.isLast();
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();

    scroller.beforeFirst();
  }

  @Override
  public void afterLast() throws SQLException {
    checkClosed();

    scroller.afterLast();
  }

  @Override
  public boolean first() throws SQLException {
    checkClosed();

    return scroller.first();
  }

  @Override
  public boolean last() throws SQLException {
    checkClosed();

    return scroller.last();
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();

    return scroller.getRow();
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkClosed();

    return scroller.absolute(row);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClosed();

    return scroller.relative(rows);
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    return scroller.next();
  }

  @Override
  public boolean previous() throws SQLException {
    checkClosed();

    return scroller.previous();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    checkClosed();

    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    checkClosed();

    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    checkClosed();

    // TODO should be SQLFeatureNotSupportedException?

    return false;
  }

  @Override
  public void insertRow() throws SQLException {
    checkClosed();

    scroller.insert();
  }

  @Override
  public void updateRow() throws SQLException {
    checkClosed();
    checkRow();

    scroller.update();
  }

  @Override
  public void deleteRow() throws SQLException {
    checkClosed();
    checkRow();

    scroller.delete();
  }

  @Override
  public void refreshRow() throws SQLException {
    checkClosed();
    checkRow();

    scroller.refresh();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    checkClosed();
    checkRow();

    scroller.cancel();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    checkClosed();
    checkUpdatable();

    scroller.createInsertRowData();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    checkClosed();
    checkUpdatable();

    scroller.cancel();
  }

  @Override
  public boolean isClosed() {
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

    if (scroller != null)
      scroller.close();

    if (housekeeper != null)
      housekeeper.remove(cleanupKey);

    statement = null;
    scroller = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    checkClosed();

    return scroller.getCursorName();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();
    return new PGResultSetMetaData(statement.connection, scroller.getResultFields(), context.getCustomTypeMap());
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

    return getVal(columnIndex, context, String.class, null);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Boolean.class, null), false);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Byte.class, null), (byte)0);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Short.class, null), (short)0);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Integer.class, null), 0);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Long.class, null), 0L);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Float.class, null), 0.0f);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return firstNonNull(getVal(columnIndex, context, Double.class, null), 0.0);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, BigDecimal.class, scale);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, BigDecimal.class, null);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    try (InputStream data = getVal(columnIndex, context, InputStream.class, null)) {

      if (data == null) {
        return null;
      }

      return ByteStreams.toByteArray(data);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {

    return getDate(columnIndex, null);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {

    return getTime(columnIndex, null);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {

    return getTimestamp(columnIndex, null);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Date.class, cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Time.class, cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Timestamp.class, cal);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Array.class, null);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, URL.class, null);
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
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {

    String data = getString(columnIndex);
    if (data == null)
      return null;

    return new ByteArrayInputStream(data.getBytes(UTF_8));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, InputStream.class, null);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Blob.class, null);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, Clob.class, null);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, SQLXML.class, null);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getObj(columnIndex, context, null, null);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> typeMap) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    TypeMapContext typeMapContext = TYPE_MAP_CONTEXTS.get();
    typeMapContext.reset(context, typeMap);

    return getObj(columnIndex, typeMapContext, null, null);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, type, null);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    checkClosed();
    checkRow();
    checkColumnIndex(columnIndex);

    return getVal(columnIndex, context, RowId.class, null);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
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

    ResultField[] resultFields = scroller.getResultFields();

    for (int c = 0; c < resultFields.length; ++c) {

      if (resultFields[c].getName().equalsIgnoreCase(columnLabel)) {
        return c + 1;
      }
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
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
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
  @Deprecated
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
  public Object getObject(String columnLabel, Map<String, Class<?>> typeMap) throws SQLException {
    return getObject(findColumn(columnLabel), typeMap);
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
    return getDate(findColumn(columnLabel), cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
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
    checkClosed();
    checkUpdate();
    set(columnIndex, null, null);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, x, null);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkClosed();
    checkUpdate();

    if (x != null) {

      x = ByteStreams.limit(x, length);
    }
    else if (length != 0) {
      throw new SQLException("Invalid length");
    }

    set(columnIndex, x, null);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkClosed();
    checkUpdate();

    if (x != null) {

      x = ByteStreams.limit(x, length);
    }
    else if (length != 0) {
      throw new SQLException("Invalid length");
    }

    set(columnIndex, x, null);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    checkClosed();
    checkUpdate();
    updateAsciiStream(columnIndex, x, (long) -1);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkClosed();
    checkUpdate();
    updateAsciiStream(columnIndex, x, (long) length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    try {
      set(columnIndex, x != null ? new String(ByteStreams.toByteArray(x), US_ASCII) : null, null);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkClosed();
    checkUpdate();
    updateCharacterStream(columnIndex, x, (long) -1);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    checkClosed();
    checkUpdate();
    updateCharacterStream(columnIndex, x, (long) length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    try {
      set(columnIndex, x != null ? CharStreams.toString(x) : null, null);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, unwrapBlob(statement.connection, x), null);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x) throws SQLException {
    checkClosed();
    checkUpdate();

    Blob blob = statement.connection.createBlob();

    try {
      ByteStreams.copy(x, blob.setBinaryStream(1));
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    set(columnIndex, blob, null);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    updateBlob(columnIndex, ByteStreams.limit(x, length));
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, unwrapClob(statement.connection, x), null);
  }

  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    checkClosed();
    checkUpdate();

    Clob clob = statement.connection.createClob();

    try {
      CharStreams.copy(x, clob.setCharacterStream(1));
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    set(columnIndex, clob, null);
  }

  @Override
  public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    updateClob(columnIndex, CharStreams.limit(x, length));
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    checkClosed();
    checkUpdate();
    updateObject(columnIndex, x, 0);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, unwrapObject(statement.connection, x), scaleOrLength);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    checkClosed();
    checkUpdate();
    set(columnIndex, unwrapRowId(x), null);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    updateNull(findColumn(columnLabel));
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    updateBoolean(findColumn(columnLabel), x);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    updateByte(findColumn(columnLabel), x);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    updateShort(findColumn(columnLabel), x);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    updateInt(findColumn(columnLabel), x);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    updateLong(findColumn(columnLabel), x);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    updateFloat(findColumn(columnLabel), x);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    updateDouble(findColumn(columnLabel), x);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    updateBigDecimal(findColumn(columnLabel), x);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    updateString(findColumn(columnLabel), x);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    updateBytes(findColumn(columnLabel), x);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    updateDate(findColumn(columnLabel), x);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    updateTime(findColumn(columnLabel), x);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    updateTimestamp(findColumn(columnLabel), x);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    updateBlob(findColumn(columnLabel), x);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    updateArray(findColumn(columnLabel), x);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
    updateSQLXML(findColumn(columnLabel), x);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    updateObject(findColumn(columnLabel), x);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    updateObject(findColumn(columnLabel), x);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream x) throws SQLException {
    updateBlob(findColumn(columnLabel), x);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream x, long length) throws SQLException {
    updateBlob(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    updateClob(findColumn(columnLabel), x);
  }

  @Override
  public void updateClob(String columnLabel, Reader x) throws SQLException {
    updateClob(findColumn(columnLabel), x);
  }

  @Override
  public void updateClob(String columnLabel, Reader x, long length) throws SQLException {
    updateClob(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    updateAsciiStream(findColumn(columnLabel), x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    updateAsciiStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    updateAsciiStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    updateBinaryStream(findColumn(columnLabel), x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    updateBinaryStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    updateBinaryStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
    updateCharacterStream(findColumn(columnLabel), x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
    updateCharacterStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
    updateCharacterStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    updateRef(findColumn(columnLabel), x);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    updateRowId(findColumn(columnLabel), x);
  }

  @Override
  public void updateNString(String columnLabel, String x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(String columnLabel, NClob x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(String columnLabel, Reader x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNClob(String columnLabel, Reader x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
    checkClosed();
    checkUpdate();
    throw NOT_SUPPORTED;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkClosed();

    return warningChain;
  }

  void addWarnings(SQLWarning warningChain) {

    this.warningChain = ErrorUtils.chainWarnings(this.warningChain, warningChain);
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
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
    throw NOT_IMPLEMENTED;
  }
}

/**
 * An implementation of different scrolling functionality required by JDBC resultsets.
 */
abstract class Scroller {

  abstract void close() throws SQLException;

  abstract String getCursorName();

  abstract int getType();

  abstract int getConcurrency();

  abstract int getHoldability();

  abstract ResultField[] getResultFields();

  abstract boolean isValidRow();

  abstract int getRow() throws SQLException;

  abstract Object getRowField(int fieldIndex, Context context, Class<?> targetType, Object targetContext) throws IOException;

  abstract RowData getRowData();

  abstract UpdatableRowData getUpdatableRowData();

  abstract void createInsertRowData() throws SQLException;

  abstract boolean isBeforeFirst() throws SQLException;

  abstract boolean isAfterLast() throws SQLException;

  abstract boolean isFirst() throws SQLException;

  abstract boolean isLast() throws SQLException;

  abstract void beforeFirst() throws SQLException;

  abstract void afterLast() throws SQLException;

  abstract boolean first() throws SQLException;

  abstract boolean last() throws SQLException;

  abstract boolean absolute(int row) throws SQLException;

  abstract boolean relative(int rows) throws SQLException;

  abstract boolean next() throws SQLException;

  abstract boolean previous() throws SQLException;

  abstract void insert() throws SQLException;

  abstract void update() throws SQLException;

  abstract void delete() throws SQLException;

  abstract void refresh() throws SQLException;

  abstract void cancel() throws SQLException;

}

/**
 * A forward-only scroller that scrolls through a single list of results
 */
class ListScroller extends Scroller {

  int currentRowIndex = -1;
  RowDataSet results;
  ResultField[] resultFields;
  private boolean releaseResults;

  ListScroller(ResultField[] resultFields, RowDataSet results, boolean releaseResults) {
    this.resultFields = resultFields;
    this.results = results;
    this.releaseResults = releaseResults;
  }

  void setResults(RowDataSet results) {
    if (this.results != null && releaseResults) {
      release(this.results);
    }
    this.results = results;
  }

  @Override
  void close() throws SQLException {
    if (releaseResults) {
      setResults(null);
    }
  }

  @Override
  ResultField[] getResultFields() {
    return resultFields;
  }

  @Override
  boolean isValidRow() {
    return currentRowIndex >= 0 && currentRowIndex < results.size();
  }

  @Override
  String getCursorName() {
    return null;
  }

  @Override
  int getType() {
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  @Override
  int getConcurrency() {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  int getHoldability() {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  int getRow() {

    if (!isValidRow())
      return 0;

    return currentRowIndex + 1;
  }

  @Override
  Object getRowField(int fieldIndex, Context context, Class<?> targetType, Object targetContext) throws IOException {
    return getRowData().getField(fieldIndex, resultFields[fieldIndex], context, targetType, targetContext);
  }

  @Override
  RowData getRowData() {
    return results.borrow(currentRowIndex);
  }

  @Override
  UpdatableRowData getUpdatableRowData() {
    return null;
  }

  @Override
  void createInsertRowData() throws SQLException {
    throw RS_NOT_UPDATABLE;
  }

  @Override
  boolean isBeforeFirst() throws SQLException {
    return !results.isEmpty() && currentRowIndex == -1;
  }

  @Override
  boolean isAfterLast() throws SQLException {
    return !results.isEmpty() && currentRowIndex == results.size();
  }

  @Override
  boolean isFirst() throws SQLException {
    return !results.isEmpty() && currentRowIndex == 0;
  }

  @Override
  boolean isLast() throws SQLException {
    return !results.isEmpty() && currentRowIndex == (results.size() - 1);
  }

  @Override
  void beforeFirst() throws SQLException {
    currentRowIndex = -1;
  }

  @Override
  void afterLast() throws SQLException {
    currentRowIndex = results.size();
  }

  @Override
  boolean first() throws SQLException {
    return absolute(1);
  }

  @Override
  boolean last() throws SQLException {
    return absolute(-1);
  }

  @Override
  boolean absolute(int row) throws SQLException {

    if (row < 0) {
      row = results.size() + 1 + row;
    }

    currentRowIndex = max(-1, min(results.size(), row - 1));

    return isValidRow();
  }

  @Override
  boolean relative(int rows) throws SQLException {

    currentRowIndex = max(-1, min(results.size(), currentRowIndex + rows));

    return isValidRow();
  }

  @Override
  boolean next() throws SQLException {
    return relative(1);
  }

  @Override
  boolean previous() throws SQLException {
    return relative(-1);
  }

  @Override
  void insert() throws SQLException {
    throw RS_NOT_UPDATABLE;
  }

  @Override
  void update() throws SQLException {
    throw RS_NOT_UPDATABLE;
  }

  @Override
  void delete() throws SQLException {
    throw RS_NOT_UPDATABLE;
  }

  @Override
  void refresh() {
  }

  @Override
  void cancel() {
  }

}

/**
 * Forward-only scroller that operates on finite batches from the server.
 *
 * Implemented using PostgreSQL portals; specifically the "PortalSuspended" functionality.
 */
class QueryScroller extends ListScroller {

  private PGResultSet resultSet;
  private int resultsIndexOffset;
  private Query query;

  QueryScroller(PGResultSet resultSet, Query query, ResultField[] resultFields, RowDataSet results) {
    super(resultFields, results, true);
    this.resultSet = resultSet;
    this.query = query;
  }

  @Override
  void close() throws SQLException {
    super.close();
    query.dispose(resultSet.statement.connection);
  }

  @Override
  int getType() {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getRow() {

    if (!isValidRow())
      return 0;

    return resultsIndexOffset + currentRowIndex + 1;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return super.isBeforeFirst() && resultsIndexOffset == 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return super.isAfterLast() && query.getStatus() == Completed;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return super.isFirst() && resultsIndexOffset == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    return super.isLast() && query.getStatus() == Completed;
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public void afterLast() throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public boolean first() throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public boolean last() throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

  @Override
  public boolean next() throws SQLException {

    if (min(++currentRowIndex, results.size()) == results.size()) {

      if (query != null && query.getStatus() != Completed) {

        Integer fetchSize = resultSet.fetchSize();
        if (fetchSize != null)
          query.setMaxRows(fetchSize);

        SQLWarning warningChain = query.execute(resultSet.statement.connection);
        resultSet.addWarnings(warningChain);

        List<ResultBatch> resultBatches = query.getResultBatches();
        if (resultBatches.size() != 1) {
          throw new SQLException("Invalid result data");
        }

        try (ResultBatch resultBatch = resultBatches.remove(0)) {

          resultFields = resultBatch.getFields();
          setResults(resultBatch.takeRows());

          resultsIndexOffset += currentRowIndex;
          currentRowIndex = -1;

          return next();
        }
      }

    }

    return isValidRow();
  }

  @Override
  boolean previous() throws SQLException {
    throw CURSOR_NOT_SCROLLABLE;
  }

}

/**
 * A forward/backward scroller that uses SQL cursors. It only ever contains a single row at any one time.
 */
class CursorScroller extends Scroller {

  private PGDirectConnection connection;
  private String cursorName;
  private int type;
  private int holdability;
  private ResultField[] resultFields;
  private int rowIndexValue;
  private int lastRowIndexValue;
  private Integer rowCountCache;
  private int rowIndexSign;
  private RowData result;

  CursorScroller(PGResultSet resultSet, String cursorName, int type, int holdability, ResultField[] resultFields) {
    this.connection = resultSet.statement.connection;
    this.cursorName = cursorName;
    this.type = type;
    this.holdability = holdability;
    this.resultFields = resultFields;
    setRowIndex(0, true);
  }

  private void setRowIndex(int value, boolean sign) {
    rowIndexValue = value;
    lastRowIndexValue = rowIndexValue;
    rowIndexSign = sign ? 1 : -1;
  }

  void setResult(RowData result) {
    release(this.result);
    this.result = result;
  }

  private boolean fetch(String type, Object loc) throws SQLException {

    String sb = "FETCH " + type + " " + loc + " FROM " + cursorName;
    setResult(connection.executeForResult(sb));

    return result != null;
  }

  private int move(String type, Object loc) throws SQLException {

    setResult(null);

    String sb = "MOVE " + type + " " + loc + " IN " + cursorName;
    return (int) connection.executeForRowsAffected(sb);
  }

  private int getRealRowCount() throws SQLException {
    if (rowCountCache == null) {
      move("ABSOLUTE", 0);
      rowCountCache = move("FORWARD", "ALL");
      move("ABSOLUTE", getRow());
    }
    return rowCountCache;
  }

  @Override
  void close() throws SQLException {

    cancel();
    setResult(null);

    if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {

      connection.execute((timeout) -> {
        QueryResult handler = new QueryResult();
        connection.getRequestExecutor().query("CLOSE " + cursorName, handler);
        handler.await(timeout, MILLISECONDS);
        handler.getBatch().close();
      });
    }

  }

  @Override
  ResultField[] getResultFields() {
    return resultFields;
  }

  @Override
  boolean isValidRow() {
    return result != null && rowIndexValue != Integer.MAX_VALUE;
  }

  @Override
  String getCursorName() {
    return cursorName;
  }

  @Override
  public int getRow() throws SQLException {

    if (!isValidRow())
      return 0;

    if (rowIndexSign < 0) {
      return getRealRowCount() - rowIndexValue + 1;
    }
    else {
      return rowIndexValue;
    }
  }

  @Override
  Object getRowField(int fieldIndex, Context context, Class<?> targetType, Object targetContext) throws IOException {
    return getRowData().getField(fieldIndex, resultFields[fieldIndex], context, targetType, targetContext);
  }

  @Override
  RowData getRowData() {
    return result;
  }

  @Override
  UpdatableRowData getUpdatableRowData() {
    if (result == null || result instanceof UpdatableRowData) {
      return (UpdatableRowData) result;
    }

    UpdatableRowData updatableResult = result.duplicateForUpdate();
    release(result);
    result = updatableResult;

    return updatableResult;
  }

  @Override
  void createInsertRowData() {
    setResult(new FieldBuffersRowData(resultFields, connection.getAllocator()));
    rowIndexValue = Integer.MAX_VALUE;
  }

  @Override
  int getType() {
    return type;
  }

  @Override
  int getConcurrency() {
    return ResultSet.CONCUR_UPDATABLE;
  }

  @Override
  int getHoldability() {
    return holdability;
  }

  @Override
  public boolean isBeforeFirst() {
    return (rowCountCache == null || rowCountCache != 0) && rowIndexValue == 0 && rowIndexSign == 1;
  }

  @Override
  public boolean isAfterLast() {
    return (rowCountCache == null || rowCountCache != 0) && rowIndexValue == 0 && rowIndexSign == -1;
  }

  @Override
  public boolean isFirst() throws SQLException {
    if (!isValidRow())
      return false;

    if (rowIndexSign == 1) {
      return rowIndexValue == 1;
    }

    if (rowCountCache == null) {
      boolean isFirst = move("RELATIVE", -1) == 0;
      move("RELATIVE", 1);
      return isFirst;
    }

    return rowIndexValue == rowCountCache;
  }

  @Override
  public boolean isLast() throws SQLException {
    if (type == ResultSet.TYPE_FORWARD_ONLY) {
      throw new SQLFeatureNotSupportedException("cannot call isLast on forward-only cursors");
    }

    if (!isValidRow())
      return false;

    if (rowIndexSign == -1) {
      return rowIndexValue == 1;
    }

    if (rowCountCache == null) {
      boolean isLast = move("RELATIVE", 1) == 0;
      move("RELATIVE", -1);
      return isLast;
    }

    return rowIndexValue == rowCountCache;
  }

  @Override
  public void beforeFirst() throws SQLException {
    move("ABSOLUTE", 0);
    setRowIndex(0, true);
  }

  @Override
  public void afterLast() throws SQLException {
    rowIndexValue += (move("FORWARD", "ALL") * rowIndexSign);
  }

  @Override
  public boolean first() throws SQLException {
    if (fetch("FIRST", "")) {
      setRowIndex(1, true);
      return true;
    }
    else {
      setRowIndex(0, true);
      rowCountCache = 0;
      return false;
    }
  }

  @Override
  public boolean last() throws SQLException {
    if (fetch("LAST", "")) {
      setRowIndex(1, false);
      return true;
    }
    else {
      setRowIndex(0, true);
      rowCountCache = 0;
      return false;
    }
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    if (fetch("ABSOLUTE", row)) {
      setRowIndex(Math.abs(row), row > 0);
      return true;
    }
    setRowIndex(0, row == 0 || row < 0);
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    if (fetch("RELATIVE", rows)) {
      rowIndexValue += (rows * rowIndexSign);
      return true;
    }
    else if (rows < 0) {
      setRowIndex(0, true);
    }
    else if (rows == 1) {
      if (rowIndexSign > 0) {
        rowCountCache = rowIndexValue;
      }
      setRowIndex(0, false);
    }
    else {
      setRowIndex(0, true);
    }
    return false;
  }

  @Override
  public boolean next() throws SQLException {
    return relative(1);
  }

  @Override
  boolean previous() throws SQLException {
    return relative(-1);
  }

  @Override
  void insert() throws SQLException {

    if (!(result instanceof UpdatableRowData) || rowIndexValue != Integer.MAX_VALUE) {
      throw new PGSQLSimpleException("not on insert row");
    }

    UpdatableRowData rowData = (UpdatableRowData) result;

    Type relType = connection.getRegistry().loadRelationType(resultFields[0].getRelationId());

    StringBuilder sb = new StringBuilder("INSERT INTO ");

    sb.append('"').append(relType.getName()).append('"');

    sb.append(" VALUES (");

    for (int pid = 0; pid < resultFields.length; ++pid) {
      sb.append("$");
      sb.append(pid + 1);
      if (pid < resultFields.length - 1) {
        sb.append(", ");
      }
    }

    sb.append(")");

    ByteBuf[] paramBuffers = rowData.getFieldBuffers();

    connection.executeForRowsAffected(sb.toString(), resultFields, paramBuffers);
  }

  @Override
  void update() throws SQLException {

    if (!(result instanceof UpdatableRowData) || rowIndexValue == Integer.MAX_VALUE) {
      throw new SQLException("not on update row");
    }

    UpdatableRowData rowData = (UpdatableRowData) result;

    Type relType = connection.getRegistry().loadRelationType(resultFields[0].getRelationId());

    StringBuilder sb = new StringBuilder("UPDATE ");

    sb.append('"').append(relType.getName()).append('"');

    sb.append(" SET ");

    for (int pid = 0; pid < resultFields.length; ++pid) {
      sb.append(resultFields[pid].getName());
      sb.append(" = $");
      sb.append(pid + 1);
      if (pid < resultFields.length - 1) {
        sb.append(", ");
      }
    }

    sb.append("WHERE CURRENT OF ");
    sb.append(cursorName);

    ByteBuf[] paramBuffers = rowData.getFieldBuffers();

    connection.executeForRowsAffected(sb.toString(), resultFields, paramBuffers);
  }

  @Override
  void delete() throws SQLException {

    if (!isValidRow())
      throw ROW_INDEX_OUT_OF_BOUNDS;

    Type relType = connection.getRegistry().loadRelationType(resultFields[0].getRelationId());

    String sql = "DELETE FROM " + '"' + relType.getName() + '"' + " WHERE CURRENT OF " + cursorName;
    long rows = connection.executeForRowsAffected(sql);
    if (rows != 0) {
      if (rowCountCache != null) {
        rowCountCache--;
      }
      rowIndexValue--;
      refresh();
    }
  }

  @Override
  void refresh() throws SQLException {
    relative(0);
  }

  @Override
  void cancel() throws SQLException {
    rowIndexValue = lastRowIndexValue;
    if (result instanceof UpdatableRowData) {
      refresh();
    }
  }

}
