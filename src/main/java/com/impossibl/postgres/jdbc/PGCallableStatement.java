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

import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultBatches;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;
import static java.util.Collections.nCopies;

import io.netty.util.ReferenceCountUtil;


public class PGCallableStatement extends PGPreparedStatement implements CallableStatement {


  private enum ParameterMode {
    In, Out, InOut, Assign
  }


  private String fullSqlText;
  private List<ParameterMode> allParameterModes;
  private Map<Integer, Integer> outParameterSQLTypes;
  private ResultField[] outParameterFields;
  private RowData outParameterData;
  private Map<String, Class<?>> typeMap;
  private Boolean nullFlag;

  private static final Map<Integer, Pattern> PARAM_REPLACE_REGEXES = new ConcurrentHashMap<>();
  private static final Pattern CLEANUP_LEADING_COMMAS_REGEX = Pattern.compile("\\(\\s*,+");
  private static final Pattern CLEANUP_MIDDLE_COMMAS_REGEX = Pattern.compile(",\\s*,");
  private static final Pattern CLEANUP_TAILING_COMMAS_REGEX = Pattern.compile(",+\\s*\\)");

  PGCallableStatement(PGDirectConnection connection, int type, int concurrency, int holdability, String sqlText, int parameterCount, String cursorName, boolean hasAssign) throws SQLException {
    super(connection, type, concurrency, holdability, sqlText, 0, cursorName);

    typeMap = connection.getTypeMap();
    fullSqlText = sqlText;
    allParameterModes = new ArrayList<>(nCopies(parameterCount, null));
    parameterTypes = EMPTY_TYPES;
    parameterFormats = EMPTY_FORMATS;
    parameterBuffers = EMPTY_BUFFERS;
    outParameterSQLTypes = new HashMap<>();
    outParameterFields = null;
    outParameterData = null;

    if (hasAssign) {
      allParameterModes.add(0, ParameterMode.Assign);
    }
  }

  @Override
  void internalClose() throws SQLException {
    super.internalClose();

    ReferenceCountUtil.release(outParameterData);
    outParameterData = null;
  }

  @Override
  void verifyParameterSet() {
  }

  @Override
  void parseIfNeeded() throws SQLException {

    sqlText = fullSqlText;

    int reSeq = 1;
    for (int c = 0; c < allParameterModes.size(); ++c) {

      Matcher matcher = getRegexForParameter(c + 1).matcher(sqlText);
      if (allParameterModes.get(c) == ParameterMode.Out) {  // redact parameter
        sqlText = matcher.replaceFirst("$2");
      }
      else {  // re-sequence parameter
        sqlText = matcher.replaceFirst("\\$" + (reSeq++) + "$2");
      }
    }

    sqlText = CLEANUP_LEADING_COMMAS_REGEX.matcher(sqlText).replaceAll("(");
    sqlText = CLEANUP_MIDDLE_COMMAS_REGEX.matcher(sqlText).replaceAll(",");
    sqlText = CLEANUP_TAILING_COMMAS_REGEX.matcher(sqlText).replaceAll(")");

    super.parseIfNeeded();
  }

  private static Pattern getRegexForParameter(int paramIndex) {

    Pattern pattern = PARAM_REPLACE_REGEXES.get(paramIndex);
    if (pattern == null) {
      pattern = Pattern.compile("\\s*\\$(" + paramIndex + ")\\s*([,)])");
      PARAM_REPLACE_REGEXES.put(paramIndex, pattern);
    }

    return pattern;
  }

  @Override
  public boolean execute() throws SQLException {

    resultBatches = ResultBatches.releaseAll(resultBatches);
    ReferenceCountUtil.release(outParameterData);
    outParameterData = null;

    boolean res = super.execute();

    if (!outParameterSQLTypes.isEmpty()) {

      if (!resultBatches.isEmpty()) {

        try (ResultBatch returnValuesBatch = resultBatches.remove(0)) {

          if (returnValuesBatch.getFields().length != outParameterSQLTypes.size() || returnValuesBatch.borrowRows().size() > 1) {
            throw new SQLException("incorrect number of out parameters");
          }

          if (!returnValuesBatch.isEmpty()) {
            outParameterFields = returnValuesBatch.getFields();
            outParameterData = returnValuesBatch.borrowRows().take(0);
          }

        }
      }
      else {

        throw new SQLException("out parameters not returned");
      }

    }

    if (!resultBatches.isEmpty()) {
      resultBatches.get(0).clearRowsAffected();
    }

    return false;
  }

  @Override
  public void clearParameters() throws SQLException {
    super.clearParameters();

    ReferenceCountUtil.release(outParameterData);
    outParameterData = null;
  }

  private int mapToInParameterIndex(int parameterIdx) {

    int inParameterIdx = parameterIdx;

    for (int allParameterIdx = 1; allParameterIdx < allParameterModes.size() && allParameterIdx <= parameterIdx; ++allParameterIdx) {
      ParameterMode mode = allParameterModes.get(allParameterIdx - 1);
      if (mode == ParameterMode.Out || mode == ParameterMode.Assign) {
        inParameterIdx--;
      }
    }

    return inParameterIdx;
  }

  private int mapToOutParameterIndex(int parameterIdx) throws SQLException {

    if (parameterIdx < 1 || parameterIdx > allParameterModes.size()) {
      throw PARAMETER_INDEX_OUT_OF_BOUNDS;
    }

    if (allParameterModes.get(parameterIdx - 1) == ParameterMode.In) {
      throw new SQLException("parameter not available");
    }

    int outParameterIdx = parameterIdx;

    for (int allParameterIdx = 1; allParameterIdx < allParameterModes.size() && allParameterIdx <= parameterIdx; ++allParameterIdx) {
      ParameterMode mode = allParameterModes.get(allParameterIdx - 1);
      if (mode == ParameterMode.In || mode == null) {
        outParameterIdx--;
      }
    }

    return outParameterIdx;
  }

  private int mapFromOutParameterIndex(int outParameterIdx) {

    int parameterIdx = 0;

    for (int allParameterIdx = 1; allParameterIdx < allParameterModes.size() && allParameterIdx <= outParameterIdx; ++allParameterIdx) {
      if (allParameterModes.get(allParameterIdx - 1) == ParameterMode.In) {
        parameterIdx++;
      }
    }

    return parameterIdx;
  }

  private <R> R getVal(int parameterIdx, Class<R> targetClass, Object targetContext) throws SQLException {
    return targetClass.cast(getObj(parameterIdx, targetClass, targetContext));
  }

  private Object getObj(int parameterIdx, Class<?> targetClass, Object targetContext) throws SQLException {

    if (query == null) {
      throw new PGSQLSimpleException("statement not executed");
    }

    if (outParameterData == null) {
      throw new PGSQLSimpleException("No parameter results");
    }

    parameterIdx--;

    if (targetClass == null) {
      Type type = SQLTypeMetaData.getType(null, outParameterSQLTypes.get(parameterIdx), connection.getRegistry());
      if (type != null) {
        targetClass = type.getCodec(type.getResultFormat()).getDecoder().getDefaultClass();
      }
    }

    Object val;
    try {
      val = outParameterData.getField(parameterIdx, outParameterFields[parameterIdx], connection, targetClass, targetContext);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    nullFlag = val == null;

    return val;
  }

  @Override
  void set(int parameterIdx, Object val, int targetSQLType) throws SQLException {
    set(parameterIdx, val, null, targetSQLType);
  }

  void set(int parameterIdx, Object source, Object sourceContext, int targetSQLType) throws SQLException {

    ParameterMode mode = allParameterModes.get(parameterIdx - 1);
    if (mode == ParameterMode.Out) {
      allParameterModes.set(parameterIdx - 1, ParameterMode.InOut);
    }
    else if (mode == null) {
      allParameterModes.set(parameterIdx - 1, ParameterMode.In);
    }

    parameterIdx = mapToInParameterIndex(parameterIdx);

    int needed = parameterIdx > parameterBuffers.length ? parameterIdx - parameterBuffers.length : 0;

    parameterTypes = copyOf(parameterTypes, parameterTypes.length + needed);
    parameterTypesParsed = copyOf(parameterTypes, parameterTypes.length + needed);
    fill(parameterTypesParsed, connection.getRegistry().loadType("text"));
    parameterFormats = copyOf(parameterFormats, parameterFormats.length + needed);
    parameterBuffers = copyOf(parameterBuffers, parameterBuffers.length + needed);

    super.set(parameterIdx, source, sourceContext, targetSQLType);

  }

  private int findParameter(String parameterName) throws SQLException {
    if (outParameterFields == null) {
      throw new PGSQLSimpleException("Unknown parameter name");
    }

    int idx;
    for (idx = 0; idx < outParameterFields.length; ++idx) {
      if (outParameterFields[idx].getName().equalsIgnoreCase(parameterName)) {
        break;
      }
    }

    if (idx == outParameterFields.length) {
      throw new PGSQLSimpleException("Unknown parameter name");
    }

    return mapFromOutParameterIndex(idx);
  }

  @Override
  boolean allowBatchSelects() {
    return true;
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    checkClosed();

    ParameterMode mode = allParameterModes.get(parameterIndex - 1);
    if (mode == ParameterMode.In) {
      allParameterModes.set(parameterIndex - 1, ParameterMode.InOut);
    }
    else if (mode == null) {
      allParameterModes.set(parameterIndex - 1, ParameterMode.Out);
    }

    outParameterSQLTypes.put(parameterIndex - 1, sqlType);
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    registerOutParameter(parameterIndex, sqlType);
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
    registerOutParameter(parameterIndex, sqlType);
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    registerOutParameter(findParameter(parameterName), sqlType);
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
    registerOutParameter(findParameter(parameterName), sqlType, scale);
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
    registerOutParameter(findParameter(parameterName), sqlType, typeName);
  }

  @Override
  public boolean wasNull() throws SQLException {
    checkClosed();

    if (nullFlag == null)
      throw new SQLException("no column fetched");

    return nullFlag;
  }

  @Override
  public String getString(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return getVal(parameterIndex, String.class, null);
  }

  @Override
  public boolean getBoolean(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Boolean.class, null), true);
  }

  @Override
  public byte getByte(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Byte.class, null), (byte) 0);
  }

  @Override
  public short getShort(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Short.class, null), (short) 0);
  }

  @Override
  public int getInt(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Integer.class, null), 0);
  }

  @Override
  public long getLong(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Long.class, null), (long) 0);
  }

  @Override
  public float getFloat(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Float.class, null), (float) 0);
  }

  @Override
  public double getDouble(int parameterIndex) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, Double.class, null), (double) 0);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    checkClosed();
    parameterIndex = mapToOutParameterIndex(parameterIndex);

    return firstNonNull(getVal(parameterIndex, BigDecimal.class, scale), BigDecimal.ZERO);
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    checkClosed();

    return firstNonNull(getVal(parameterIndex, BigDecimal.class, null), BigDecimal.ZERO);
  }

  @Override
  public byte[] getBytes(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, byte[].class, null);
  }

  @Override
  public Date getDate(int parameterIndex) throws SQLException {

    return getDate(parameterIndex, Calendar.getInstance());
  }

  @Override
  public Time getTime(int parameterIndex) throws SQLException {

    return getTime(parameterIndex, Calendar.getInstance());
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex) throws SQLException {

    return getTimestamp(parameterIndex, Calendar.getInstance());
  }

  @Override
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Date.class, cal);
  }

  @Override
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Time.class, cal);
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Timestamp.class, cal);
  }

  @Override
  public Array getArray(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Array.class, null);
  }

  @Override
  public URL getURL(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, URL.class, null);
  }

  @Override
  public Reader getCharacterStream(int parameterIndex) throws SQLException {

    String data = getString(parameterIndex);
    if (data == null)
      return null;

    return new StringReader(data);
  }

  @Override
  public Blob getBlob(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Blob.class, null);
  }

  @Override
  public Clob getClob(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, Clob.class, null);
  }

  @Override
  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, SQLXML.class, null);
  }

  @Override
  public Object getObject(int parameterIndex) throws SQLException {
    return getObject(parameterIndex, typeMap);
  }

  @Override
  public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    checkClosed();

    return getObj(parameterIndex, null, map);
  }

  @Override
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, type, null);
  }

  @Override
  public RowId getRowId(int parameterIndex) throws SQLException {
    checkClosed();

    return getVal(parameterIndex, RowId.class, null);
  }

  @Override
  public Ref getRef(int parameterIndex) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public NClob getNClob(int parameterIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public String getNString(int parameterIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public String getString(String parameterName) throws SQLException {
    return getString(findParameter(parameterName));
  }

  @Override
  public boolean getBoolean(String parameterName) throws SQLException {
    return getBoolean(findParameter(parameterName));
  }

  @Override
  public byte getByte(String parameterName) throws SQLException {
    return getByte(findParameter(parameterName));
  }

  @Override
  public short getShort(String parameterName) throws SQLException {
    return getShort(findParameter(parameterName));
  }

  @Override
  public int getInt(String parameterName) throws SQLException {
    return getInt(findParameter(parameterName));
  }

  @Override
  public long getLong(String parameterName) throws SQLException {
    return getLong(findParameter(parameterName));
  }

  @Override
  public float getFloat(String parameterName) throws SQLException {
    return getFloat(findParameter(parameterName));
  }

  @Override
  public double getDouble(String parameterName) throws SQLException {
    return getDouble(findParameter(parameterName));
  }

  @Override
  public byte[] getBytes(String parameterName) throws SQLException {
    return getBytes(findParameter(parameterName));
  }

  @Override
  public Date getDate(String parameterName) throws SQLException {
    return getDate(findParameter(parameterName));
  }

  @Override
  public Time getTime(String parameterName) throws SQLException {
    return getTime(findParameter(parameterName));
  }

  @Override
  public Timestamp getTimestamp(String parameterName) throws SQLException {
    return getTimestamp(findParameter(parameterName));
  }

  @Override
  public Object getObject(String parameterName) throws SQLException {
    return getObject(findParameter(parameterName));
  }

  @Override
  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    return getBigDecimal(findParameter(parameterName));
  }

  @Override
  public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    return getObject(findParameter(parameterName));
  }

  @Override
  public Ref getRef(String parameterName) throws SQLException {
    return getRef(findParameter(parameterName));
  }

  @Override
  public Blob getBlob(String parameterName) throws SQLException {
    return getBlob(findParameter(parameterName));
  }

  @Override
  public Clob getClob(String parameterName) throws SQLException {
    return getClob(findParameter(parameterName));
  }

  @Override
  public Array getArray(String parameterName) throws SQLException {
    return getArray(findParameter(parameterName));
  }

  @Override
  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    return getDate(findParameter(parameterName));
  }

  @Override
  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    return getTime(findParameter(parameterName));
  }

  @Override
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    return getTimestamp(findParameter(parameterName));
  }

  @Override
  public URL getURL(String parameterName) throws SQLException {
    return getURL(findParameter(parameterName));
  }

  @Override
  public RowId getRowId(String parameterName) throws SQLException {
    return getRowId(findParameter(parameterName));
  }

  @Override
  public NClob getNClob(String parameterName) throws SQLException {
    return getNClob(findParameter(parameterName));
  }

  @Override
  public SQLXML getSQLXML(String parameterName) throws SQLException {
    return getSQLXML(findParameter(parameterName));
  }

  @Override
  public String getNString(String parameterName) throws SQLException {
    return getNString(findParameter(parameterName));
  }

  @Override
  public Reader getNCharacterStream(String parameterName) throws SQLException {
    return getNCharacterStream(findParameter(parameterName));
  }

  @Override
  public Reader getCharacterStream(String parameterName) throws SQLException {
    return getCharacterStream(findParameter(parameterName));
  }

  @Override
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    return getObject(findParameter(parameterName), type);
  }

  @Override
  public void setNull(String parameterName, int sqlType) throws SQLException {
    setNull(findParameter(parameterName), sqlType);
  }

  @Override
  public void setBoolean(String parameterName, boolean x) throws SQLException {
    setBoolean(findParameter(parameterName), x);
  }

  @Override
  public void setByte(String parameterName, byte x) throws SQLException {
    setByte(findParameter(parameterName), x);
  }

  @Override
  public void setShort(String parameterName, short x) throws SQLException {
    setShort(findParameter(parameterName), x);
  }

  @Override
  public void setInt(String parameterName, int x) throws SQLException {
    setInt(findParameter(parameterName), x);
  }

  @Override
  public void setLong(String parameterName, long x) throws SQLException {
    setLong(findParameter(parameterName), x);
  }

  @Override
  public void setFloat(String parameterName, float x) throws SQLException {
    setFloat(findParameter(parameterName), x);
  }

  @Override
  public void setDouble(String parameterName, double x) throws SQLException {
    setDouble(findParameter(parameterName), x);
  }

  @Override
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    setBigDecimal(findParameter(parameterName), x);
  }

  @Override
  public void setString(String parameterName, String x) throws SQLException {
    setString(findParameter(parameterName), x);
  }

  @Override
  public void setBytes(String parameterName, byte[] x) throws SQLException {
    setBytes(findParameter(parameterName), x);
  }

  @Override
  public void setDate(String parameterName, Date x) throws SQLException {
    setDate(findParameter(parameterName), x);
  }

  @Override
  public void setTime(String parameterName, Time x) throws SQLException {
    setTime(findParameter(parameterName), x);
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    setTimestamp(findParameter(parameterName), x);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    setAsciiStream(findParameter(parameterName), x, length);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    setBinaryStream(findParameter(parameterName), x, length);
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
    setObject(findParameter(parameterName), x, targetSqlType, scale);
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    setObject(findParameter(parameterName), x, targetSqlType);
  }

  @Override
  public void setObject(String parameterName, Object x) throws SQLException {
    setObject(findParameter(parameterName), x);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
    set(findParameter(parameterName), reader, length);
  }

  @Override
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    setDate(findParameter(parameterName), x, cal);
  }

  @Override
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    setTime(findParameter(parameterName), x, cal);
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    setTimestamp(findParameter(parameterName), x, cal);
  }

  @Override
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    setNull(findParameter(parameterName), sqlType, typeName);
  }

  @Override
  public void setRowId(String parameterName, RowId x) throws SQLException {
    setRowId(findParameter(parameterName), x);
  }

  @Override
  public void setNString(String parameterName, String value) throws SQLException {
    setNString(findParameter(parameterName), value);
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
    setNCharacterStream(findParameter(parameterName), value, length);
  }

  @Override
  public void setNClob(String parameterName, NClob value) throws SQLException {
    setNClob(findParameter(parameterName), value);
  }

  @Override
  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    setClob(findParameter(parameterName), reader, length);
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
    setBlob(findParameter(parameterName), inputStream, length);
  }

  @Override
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    setNClob(findParameter(parameterName), reader, length);
  }

  @Override
  public void setBlob(String parameterName, Blob x) throws SQLException {
    setBlob(findParameter(parameterName), x);
  }

  @Override
  public void setClob(String parameterName, Clob x) throws SQLException {
    setClob(findParameter(parameterName), x);
  }

  @Override
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    setSQLXML(findParameter(parameterName), xmlObject);
  }

  @Override
  public void setURL(String parameterName, URL val) throws SQLException {
    setURL(findParameter(parameterName), val);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
    setAsciiStream(findParameter(parameterName), x, length);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
    setBinaryStream(findParameter(parameterName), x, length);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
    setCharacterStream(findParameter(parameterName), reader, length);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    setAsciiStream(findParameter(parameterName), x);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    setBinaryStream(findParameter(parameterName), x);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    setCharacterStream(findParameter(parameterName), reader);
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    setNCharacterStream(findParameter(parameterName), value);
  }

  @Override
  public void setClob(String parameterName, Reader reader) throws SQLException {
    setClob(findParameter(parameterName), reader);
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    setBlob(findParameter(parameterName), inputStream);
  }

  @Override
  public void setNClob(String parameterName, Reader reader) throws SQLException {
    setNClob(findParameter(parameterName), reader);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
    throw NOT_IMPLEMENTED;
  }
}
