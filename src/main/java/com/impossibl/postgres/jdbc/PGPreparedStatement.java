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

import com.impossibl.postgres.datetime.instants.Instants;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_ALLOWED_ON_PREP_STMT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.SQLTypeMetaData.getSQLType;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapSetType;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapBlob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapClob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapObject;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapRowId;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

class PGPreparedStatement extends PGStatement implements PreparedStatement {


  String sqlText;
  List<Type> parameterTypes;
  List<Object> parameterValues;
  List<List<Type>> batchParameterTypes;
  List<List<Object>> batchParameterValues;
  boolean wantsGeneratedKeys;
  boolean parsed;


  PGPreparedStatement(PGConnectionImpl connection, int type, int concurrency, int holdability, String name, String sqlText, int parameterCount, String cursorName) {
    super(connection, type, concurrency, holdability, null, null);
    this.sqlText = sqlText;
    this.parameterTypes = new ArrayList<>(asList(new Type[parameterCount]));
    this.parameterValues = new ArrayList<>(asList(new Object[parameterCount]));
    this.cursorName = cursorName;
  }

  public boolean getWantsGeneratedKeys() {
    return wantsGeneratedKeys;
  }

  public void setWantsGeneratedKeys(boolean wantsGeneratedKeys) {
    this.wantsGeneratedKeys = wantsGeneratedKeys;
  }

  void set(int parameterIdx, Object val, int targetSQLType) throws SQLException {
    checkClosed();

    if (parameterIdx < 1 || parameterIdx > parameterValues.size()) {
      throw PARAMETER_INDEX_OUT_OF_BOUNDS;
    }

    parameterIdx -= 1;

    Type paramType = parameterTypes.get(parameterIdx);

    if (targetSQLType == Types.ARRAY || targetSQLType == Types.STRUCT || targetSQLType == Types.OTHER) {
      targetSQLType = Types.NULL;
    }

    if (paramType == null || targetSQLType != getSQLType(paramType)) {

      paramType = SQLTypeMetaData.getType(targetSQLType, connection.getRegistry());

      parameterTypes.set(parameterIdx, paramType);

      parsed = false;
    }

    parameterValues.set(parameterIdx, val);
  }

  @Override
  void internalClose() throws SQLException {

    super.internalClose();

    parameterTypes = null;
    parameterValues = null;
  }

  void parseIfNeeded() throws SQLException {

    if (cursorName != null && command != null) {
      super.executeSimple("CLOSE " + cursorName);
    }

    if (!parsed) {

      if (name != null && !name.startsWith(CACHED_STATEMENT_PREFIX)) {
        connection.execute(connection.getProtocol().createClose(ServerObjectType.Statement, name), false);
      }

      CachedStatement cachedStatement;

      try {

        final CachedStatementKey key = new CachedStatementKey(sqlText, Collections.<Type> emptyList());

        cachedStatement = connection.getCachedStatement(key, new Callable<CachedStatement>() {

          @Override
          public CachedStatement call() throws Exception {

            String name = CACHED_STATEMENT_PREFIX + Integer.toString(key.hashCode());

            PrepareCommand prep = connection.getProtocol().createPrepare(name, sqlText, Collections.<Type> emptyList());

            warningChain = connection.execute(prep, true);

            return new CachedStatement(name, prep.getDescribedParameterTypes(), prep.getDescribedResultFields());
          }

        });

      }
      catch (ExecutionException e) {
        throw (SQLException) e.getCause();
      }
      catch (Exception e) {
        throw (SQLException) e;
      }

      name = cachedStatement.name;
      parameterTypes = copyNonNullTypes(parameterTypes, cachedStatement.parameterTypes);
      resultFields = cachedStatement.resultFields;
      parsed = true;

    }

  }

  private void coerceParameters() throws SQLException {

    for (int c = 0, sz = parameterTypes.size(); c < sz; ++c) {

      Type parameterType = parameterTypes.get(c);
      Object parameterValue = parameterValues.get(c);

      if (parameterType != null && parameterValue != null) {

        Class<?> targetType = mapSetType(parameterType);

        try {
          parameterValue = coerce(parameterValue, parameterType, targetType, Collections.<String, Class<?>>emptyMap(), TimeZone.getDefault(), connection);
        }
        catch (SQLException coercionException) {
          throw new SQLException("Error converting parameter " + c, coercionException);
        }
      }

      parameterValues.set(c, parameterValue);
    }

  }

  @Override
  public boolean execute() throws SQLException {

    parseIfNeeded();
    closeResultSets();

    coerceParameters();

    boolean res = super.executeStatement(name, parameterTypes, parameterValues);

    if (cursorName != null) {
      res = super.executeSimple("FETCH ABSOLUTE 0 FROM " + cursorName);
    }

    if (wantsGeneratedKeys) {
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

    return getUpdateCount();
  }

  @Override
  public void addBatch() throws SQLException {
    checkClosed();

    if (batchParameterTypes == null) {
      batchParameterTypes = new ArrayList<>();
    }

    if (batchParameterValues == null) {
      batchParameterValues = new ArrayList<>();
    }

    coerceParameters();

    batchParameterTypes.add(new ArrayList<>(parameterTypes));
    batchParameterValues.add(new ArrayList<>(parameterValues));
  }

  @Override
  public void clearBatch() throws SQLException {
    checkClosed();

    batchParameterValues = null;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    checkClosed();
    closeResultSets();

    try {

      warningChain = null;

      if (batchParameterValues == null || batchParameterValues.isEmpty()) {
        return new int[0];
      }

      int[] counts = new int[batchParameterValues.size()];
      Arrays.fill(counts, SUCCESS_NO_INFO);

      List<Object[]> generatedKeys = new ArrayList<>();

      BindExecCommand command = connection.getProtocol().createBindExec(null, null, parameterTypes, Collections.emptyList(), resultFields, Object[].class);

      List<Type> lastParameterTypes = null;
      List<ResultField> lastResultFields = null;

      for (int c = 0, sz = batchParameterValues.size(); c < sz; ++c) {

        List<Type> parameterTypes = mergeTypes(batchParameterTypes.get(c), lastParameterTypes);

        if (lastParameterTypes == null || !lastParameterTypes.equals(parameterTypes)) {

          PrepareCommand prep = connection.getProtocol().createPrepare(null, sqlText, parameterTypes);

          connection.execute(prep, true);

          parameterTypes = prep.getDescribedParameterTypes();
          lastParameterTypes = parameterTypes;
          lastResultFields = prep.getDescribedResultFields();
        }

        List<Object> parameterValues = batchParameterValues.get(c);

        command.setParameterTypes(parameterTypes);
        command.setParameterValues(parameterValues);

        SQLWarning warnings = connection.execute(command, true);

        warningChain = chainWarnings(warningChain, warnings);

        List<QueryCommand.ResultBatch> resultBatches = command.getResultBatches();
        if (resultBatches.size() != 1) {
          throw new BatchUpdateException(counts);
        }

        QueryCommand.ResultBatch resultBatch = resultBatches.get(0);
        if (resultBatch.rowsAffected == null) {
          counts[c] = 0;
        }
        else {
          counts[c] = (int) (long) resultBatch.rowsAffected;
        }

        if (wantsGeneratedKeys) {
          generatedKeys.add((Object[]) resultBatch.results.get(0));
        }

      }

      generatedKeysResultSet = createResultSet(lastResultFields, generatedKeys);

      return counts;

    }
    finally {
      batchParameterTypes = null;
      batchParameterValues = null;
    }

  }

  List<Type> mergeTypes(List<Type> list, List<Type> defaultTypes) {

    if (defaultTypes == null)
      return list;

    for (int c = 0, sz = list.size(); c < sz; ++c) {

      Type type = list.get(c);
      if (type == null)
        type = defaultTypes.get(c);

      list.set(c, type);
    }

    return list;
  }

  List<Type> copyNonNullTypes(List<Type> list, List<Type> sourceTypes) {

    if (sourceTypes == null)
      return list;

    for (int c = 0, sz = list.size(); c < sz; ++c) {

      Type type = sourceTypes.get(c);
      if (type != null) {
        list.set(c, type);
      }
    }

    return list;
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

    parseIfNeeded();

    return new PGParameterMetaData(parameterTypes, connection.getTypeMap());
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();

    parseIfNeeded();

    return new PGResultSetMetaData(connection, resultFields, connection.getTypeMap());
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    set(parameterIndex, null, Types.NULL);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    set(parameterIndex, x, Types.BOOLEAN);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    set(parameterIndex, x, Types.TINYINT);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    set(parameterIndex, x, Types.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    set(parameterIndex, x, Types.INTEGER);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    set(parameterIndex, x, Types.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    set(parameterIndex, x, Types.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    set(parameterIndex, x, Types.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    set(parameterIndex, x, Types.DECIMAL);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    set(parameterIndex, x, Types.VARCHAR);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    set(parameterIndex, x, Types.BINARY);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    setDate(parameterIndex, x, Calendar.getInstance());
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    setTime(parameterIndex, x, Calendar.getInstance());
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    setTimestamp(parameterIndex, x, Calendar.getInstance());
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    TimeZone zone = cal.getTimeZone();

    set(parameterIndex, Instants.fromDate(x, zone), Types.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {


    TimeZone zone = cal.getTimeZone();

    set(parameterIndex, Instants.fromTime(x, zone), Types.TIME);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    checkClosed();

    TimeZone zone = cal.getTimeZone();

    set(parameterIndex, Instants.fromTimestamp(x, zone), Types.TIMESTAMP);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    set(parameterIndex, x, Types.BINARY);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    if (length < 0) {
      throw new SQLException("Invalid length");
    }

    if (x != null) {

      x = ByteStreams.limit(x, length);
    }
    else if (length != 0) {
      throw new SQLException("Invalid length");
    }

    set(parameterIndex, x, Types.BINARY);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    if (length < 0) {
      throw new SQLException("Invalid length");
    }

    if (x != null) {

      x = ByteStreams.limit(x, length);
    }
    else if (length != 0) {
      throw new SQLException("Invalid length");
    }

    set(parameterIndex, x, Types.BINARY);
  }

  @Override
  @Deprecated
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
    catch (IOException e) {
      throw new SQLException(e);
    }

    set(parameterIndex, writer.toString(), Types.VARCHAR);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    set(parameterIndex, x, Types.OTHER);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    checkClosed();

    setObject(parameterIndex, x, targetSqlType, 0);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), targetSqlType);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapBlob(connection, x), Types.BLOB);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    setBlob(parameterIndex, ByteStreams.limit(inputStream, length));
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    checkClosed();

    Blob blob = connection.createBlob();

    try {
      ByteStreams.copy(inputStream, blob.setBinaryStream(1));
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    set(parameterIndex, blob, Types.BLOB);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapClob(connection, x), Types.CLOB);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    setClob(parameterIndex, CharStreams.limit(reader, length));
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    checkClosed();

    Clob clob = connection.createClob();

    try {
      CharStreams.copy(reader, clob.setCharacterStream(1));
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    set(parameterIndex, clob, Types.CLOB);
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    set(parameterIndex, x, Types.ARRAY);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    set(parameterIndex, null, Types.NULL);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    set(parameterIndex, x, Types.VARCHAR);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    checkClosed();

    if (!(xmlObject instanceof PGSQLXML)) {
      throw new SQLException("SQLXML object not created by driver");
    }

    PGSQLXML sqlXml = (PGSQLXML) xmlObject;

    set(parameterIndex, sqlXml.getData(), Types.SQLXML);
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapRowId(connection, x), Types.ROWID);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    checkClosed();
    throw NOT_IMPLEMENTED;
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    checkClosed();
    throw NOT_SUPPORTED;
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
