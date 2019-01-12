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

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.PrepareResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.ByteBufs;
import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;
import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_ALLOWED_ON_PREP_STMT;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.NO_RESULT_COUNT_AVAILABLE;
import static com.impossibl.postgres.jdbc.Exceptions.NO_RESULT_SET_AVAILABLE;
import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.JDBCTypeMapping.getJDBCType;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapBlob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapClob;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapObject;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapRowId;
import static com.impossibl.postgres.jdbc.Unwrapping.unwrapXML;
import static com.impossibl.postgres.protocol.FieldFormat.Text;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;
import static com.impossibl.postgres.utils.ByteBufs.releaseAll;
import static com.impossibl.postgres.utils.ByteBufs.retainedDuplicateAll;

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
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static java.lang.Integer.toHexString;
import static java.lang.Long.min;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import static io.netty.buffer.ByteBufUtil.writeUtf8;

class PGPreparedStatement extends PGStatement implements PreparedStatement {


  String sqlText;
  Type[] parameterTypes;
  Type[] parameterTypesParsed;
  FieldFormat[] parameterFormats;
  ByteBuf[] parameterBuffers;
  private int parameterCount;
  private boolean[] parameterSet;
  private List<Type[]> batchParameterTypes;
  private List<FieldFormat[]> batchParameterFormats;
  private List<ByteBuf[]> batchParameterBuffers;
  private boolean wantsGeneratedKeys;
  protected boolean parsed;


  PGPreparedStatement(PGDirectConnection connection, int type, int concurrency, int holdability, String sqlText, int parameterCount, String cursorName) {
    super(connection, type, concurrency, holdability, null, null);
    this.sqlText = sqlText;
    this.parameterCount = parameterCount;
    this.parameterTypes = new Type[parameterCount];
    this.parameterFormats = new FieldFormat[parameterCount];
    this.parameterBuffers = new ByteBuf[parameterCount];
    this.parameterSet = new boolean[parameterCount];
    this.cursorName = cursorName;
  }

  void setWantsGeneratedKeys() {
    this.wantsGeneratedKeys = true;
  }

  private int checkParameterIndex(int parameterIdx) throws SQLException {

    if (parameterIdx < 1 || parameterIdx > parameterTypes.length) {
      throw PARAMETER_INDEX_OUT_OF_BOUNDS;
    }

    return parameterIdx - 1;
  }

  private Type resolveType(int parameterIdx, SQLType sqlType, Object value) throws SQLException {

    describeIfNeeded();

    Type suggestedType = JDBCTypeMapping.getType(sqlType, value, connection.getRegistry());
    Type parsedType = parameterTypesParsed[parameterIdx];

    Type type = suggestedType;
    if (suggestedType == null || parsedType.getCategory() != Type.Category.String) {
      type = parsedType;
    }

    return type;
  }

  void set(int parameterIdx, Object source, SQLType sqlType) throws SQLException {
    set(parameterIdx, source, null, sqlType);
  }

  void set(int parameterIdx, Object source, Object sourceContext, SQLType sqlType) throws SQLException {
    checkClosed();
    parameterIdx = checkParameterIndex(parameterIdx);

    Type paramType = resolveType(parameterIdx, sqlType, source);

    FieldFormat paramFormat = paramType.getCategory() == Type.Category.String ? Text : paramType.getParameterFormat();

    parameterTypes[parameterIdx] = paramType;
    parameterFormats[parameterIdx] = paramFormat;

    ReferenceCountUtil.release(parameterBuffers[parameterIdx]);
    parameterBuffers[parameterIdx] = null;

    if (source != null) {

      try {
        switch (paramFormat) {
          case Text: {
            StringBuilder out = new StringBuilder();
            paramType.getTextCodec().getEncoder().encode(connection, paramType, source, sourceContext, out);
            parameterBuffers[parameterIdx] = writeUtf8(connection.getAllocator(), out);
          }
          break;

          case Binary: {
            ByteBuf out = connection.getAllocator().buffer();
            paramType.getBinaryCodec().getEncoder().encode(connection, paramType, source, sourceContext, out);
            parameterBuffers[parameterIdx] = out;
          }
          break;
        }
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }

    }

    if (parameterCount > 0) {
      parameterSet[parameterIdx] = true;
    }
  }

  @Override
  void internalClose() throws SQLException {

    super.internalClose();

    releaseAll(parameterBuffers);
    parameterBuffers = null;

    if (batchParameterBuffers != null) {
      batchParameterBuffers.forEach(ByteBufs::releaseAll);
      batchParameterBuffers = null;
    }

    parameterTypes = null;
    parameterSet = null;
  }

  void verifyParameterSet() throws SQLException {
    if (parameterCount > 0) {
      int count = 0;
      for (Boolean b : parameterSet) {
        if (b != null && Boolean.TRUE.equals(b))
          count++;
      }
      if (count != parameterCount)
        throw new SQLException("Incorrect parameter count, was " + count + ", expected: " + parameterCount);
    }
  }

  private void describeIfNeeded() throws SQLException {

    if (parameterTypesParsed != null) {
      return;
    }

    // First, check statement cache
    StatementDescription cachedDescription = connection.getCachedStatementDescription(sqlText, () -> {

      PrepareResult result = connection.execute(timeout -> {
        PrepareResult handler = new PrepareResult();
        connection.getRequestExecutor().prepare(null, sqlText, EMPTY_TYPES, handler);
        handler.await(timeout, MILLISECONDS);
        return handler;
      });

      return new StatementDescription(result.getDescribedParameterTypes(connection), result.getDescribedResultFields());
    });

    if (cachedDescription != null) {
      parameterTypesParsed = cachedDescription.parameterTypes;
    }

  }

  void parseIfNeeded() throws SQLException {

    if (query != null) {
      closeCursor(connection, cursorName);
    }

    if (!parsed) {

      if (name != null && !name.startsWith(CACHED_STATEMENT_PREFIX)) {
        try {
          connection.getRequestExecutor().close(ServerObjectType.Statement, name);
        }
        catch (IOException ignored) {
          // Close errors can be ignored
        }
      }

      PreparedStatementDescription cachedStatement;

      final StatementCacheKey key = new StatementCacheKey(sqlText, parameterTypes);

      cachedStatement = connection.getCachedPreparedStatement(key, () -> {

        String name = connection.isCacheEnabled() ?
            CACHED_STATEMENT_PREFIX + toHexString(key.hashCode()) : NO_CACHE_STATEMENT_PREFIX + toHexString(key.hashCode());

        PrepareResult prep = connection.execute((timeout) -> {
          PrepareResult handler = new PrepareResult();
          connection.getRequestExecutor().prepare(name, sqlText, parameterTypes, handler);
          handler.await(timeout, MILLISECONDS);
          return handler;
        });

        warningChain = chainWarnings(warningChain, prep);

        // Results are always described as "Text"... update them to our preferred format.
        ResultField[] describedResultFields = prep.getDescribedResultFields().clone();
        for (ResultField describedResultField : describedResultFields) {
          Type type = connection.getRegistry().resolve(describedResultField.getTypeRef());
          if (type != null) {
            describedResultField.setFormat(type.getResultFormat());
          }
        }

        return new PreparedStatementDescription(name, prep.getDescribedParameterTypes(connection), describedResultFields);
      });

      if (cachedStatement != null) {
        name = cachedStatement.name;
        parameterTypesParsed = cachedStatement.parameterTypes;
        resultFields = cachedStatement.resultFields;
        parsed = true;
      }

    }

  }

  boolean allowBatchSelects() {
    return false;
  }

  @Override
  public boolean execute() throws SQLException {
    checkClosed();

    parseIfNeeded();
    closeResultSets();
    verifyParameterSet();

    boolean res;

    if (name == null) {
      res = super.executeDirect(sqlText, parameterFormats, parameterBuffers, resultFields);
    }
    else {
      res = super.executeStatement(name, parameterFormats, parameterBuffers);
    }

    if (cursorName != null) {
      res = super.executeDirect("FETCH ABSOLUTE 0 FROM " + cursorName, null, null, resultFields);
    }

    if (wantsGeneratedKeys) {
      generatedKeysResultSet = getResultSet();
      res = false;
    }

    return res;
  }

  @Override
  public PGResultSet executeQuery() throws SQLException {

    if (!execute()) {
      throw NO_RESULT_SET_AVAILABLE;
    }

    return getResultSet();
  }

  @Override
  public int executeUpdate() throws SQLException {

    long count = executeLargeUpdate();
    return (int) min(count, Integer.MAX_VALUE);
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    if (execute()) {
      throw NO_RESULT_COUNT_AVAILABLE;
    }

    return getLargeUpdateCount();
  }

  @Override
  public void addBatch() throws SQLException {
    checkClosed();

    if (batchParameterTypes == null) {
      batchParameterTypes = new ArrayList<>();
    }
    if (batchParameterFormats == null) {
      batchParameterFormats = new ArrayList<>();
    }
    if (batchParameterBuffers == null) {
      batchParameterBuffers = new ArrayList<>();
    }

    batchParameterTypes.add(parameterTypes.clone());
    batchParameterFormats.add(parameterFormats.clone());
    batchParameterBuffers.add(retainedDuplicateAll(parameterBuffers));
  }

  @Override
  public void clearBatch() throws SQLException {
    checkClosed();

    if (batchParameterBuffers != null) {
      batchParameterBuffers.forEach(ByteBufs::releaseAll);
      batchParameterBuffers = null;
    }

    batchParameterTypes = null;
    batchParameterFormats = null;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    checkClosed();

    IntegerBatchResults results = new IntegerBatchResults(connection.isStrictMode());
    executeBatch(results);
    return results.counts;
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    checkClosed();

    LongBatchResults results = new LongBatchResults(connection.isStrictMode());
    executeBatch(results);
    return results.counts;
  }

  private void executeBatch(BatchResults results) throws SQLException {
    closeResultSets();

    try {

      warningChain = null;

      if (batchParameterBuffers == null || batchParameterBuffers.isEmpty()) {
        return;
      }

      results.setBatchSize(batchParameterBuffers.size());

      RowDataSet generatedKeys = new RowDataSet();

      if (!connection.autoCommit && connection.getTransactionStatus() == TransactionStatus.Idle) {
        connection.execute((long timeout) -> connection.getRequestExecutor().lazyExecute("TC"));
      }

      Type[] lastParameterTypes = null;
      ResultField[] lastResultFields = null;

      int batchIdx = 0;
      int sz = batchParameterBuffers.size();

      try {
        RequestExecutor requestExecutor = connection.getRequestExecutor();

        while (batchIdx < sz) {

          Type[] suggestedParameterTypes = mergedTypes(batchParameterTypes.get(batchIdx), lastParameterTypes);

          if (lastParameterTypes == null || !Arrays.equals(lastParameterTypes, parameterTypes)) {

            PrepareResult prep = connection.execute((timeout) -> {
              PrepareResult handler = new PrepareResult();
              requestExecutor.prepare(null, sqlText, suggestedParameterTypes, handler);
              handler.await(timeout, MILLISECONDS);
              return handler;
            });

            warningChain = chainWarnings(warningChain, prep);

            parameterTypes = prep.getDescribedParameterTypes(connection);
            lastParameterTypes = parameterTypes;
            lastResultFields = prep.getDescribedResultFields();
          }

          FieldFormat[] parameterFormats = batchParameterFormats.get(batchIdx);
          ByteBuf[] parameterBuffers = batchParameterBuffers.get(batchIdx);
          ResultField[] resultFields = lastResultFields;

          ExecuteResult exec = connection.execute((timeout) -> {
            ExecuteResult handler = new ExecuteResult(resultFields);
            requestExecutor.execute(null, null, parameterFormats, parameterBuffers, resultFields, 0, handler);
            handler.await(timeout, MILLISECONDS);
            return handler;
          });

          warningChain = chainWarnings(warningChain, exec);

          try (ResultBatch resultBatch = exec.getBatch()) {

            if (!allowBatchSelects() && resultBatch.getCommand().equals("SELECT")) {
              throw results.getException(batchIdx, "SELECT in executeBatch", null);
            }
            else if (resultBatch.getRowsAffected() != null) {
              results.setUpdateCount(batchIdx, resultBatch.getRowsAffected());
            }
            else {
              results.setUpdateCount(batchIdx, SUCCESS_NO_INFO);
            }

            if (wantsGeneratedKeys) {
              generatedKeys.add(resultBatch.borrowRows().take(0));
            }
          }

          batchIdx++;
        }
      }
      catch (IOException | SQLException se) {
        throw results.getException(batchIdx, null, se);
      }

      generatedKeysResultSet = createResultSet(lastResultFields, generatedKeys, true, connection.getTypeMap());

    }
    finally {
      batchParameterTypes = null;
      if (batchParameterBuffers != null) {
        batchParameterBuffers.forEach(ByteBufs::releaseAll);
        batchParameterBuffers = null;
      }
    }

  }

  private Type[] mergedTypes(Type[] types, Type[] defaultTypes) {
    types = types.clone();
    mergeTypes(types, defaultTypes);
    return types;
  }

  private void mergeTypes(Type[] types, Type[] defaultTypes) {

    if (defaultTypes == null) return;

    for (int typeIdx = 0; typeIdx < types.length; ++typeIdx) {

      if (types[typeIdx] == null) {
        types[typeIdx] = defaultTypes[typeIdx];
      }
    }

  }

  @Override
  public void clearParameters() throws SQLException {
    checkClosed();

    releaseAll(parameterBuffers);

    for (int parameterIdx = 0; parameterIdx < parameterSet.length; ++parameterIdx) {
      parameterSet[parameterIdx] = Boolean.FALSE;
    }

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    checkClosed();

    parseIfNeeded();

    return new PGParameterMetaData(parameterTypesParsed, connection.getTypeMap());
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();

    parseIfNeeded();

    return new PGResultSetMetaData(connection, resultFields, connection.getTypeMap());
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    set(parameterIndex, null, JDBCType.valueOf(sqlType));
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    set(parameterIndex, x, JDBCType.BOOLEAN);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    set(parameterIndex, x, JDBCType.TINYINT);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    set(parameterIndex, x, JDBCType.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    set(parameterIndex, x, JDBCType.INTEGER);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    set(parameterIndex, x, JDBCType.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    set(parameterIndex, x, JDBCType.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    set(parameterIndex, x, JDBCType.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    set(parameterIndex, x, JDBCType.DECIMAL);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    set(parameterIndex, x, JDBCType.VARCHAR);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    set(parameterIndex, x, JDBCType.BINARY);
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
    set(parameterIndex, x, cal, JDBCType.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    set(parameterIndex, x, cal, JDBCType.TIME);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    set(parameterIndex, x, cal, JDBCType.TIMESTAMP);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    set(parameterIndex, x, JDBCType.BINARY);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    if (length < 0) {
      throw new SQLException("Invalid length");
    }

    if (x == null  && length != 0) {
      throw new SQLException("Invalid length");
    }

    set(parameterIndex, x, (long) length, JDBCType.BINARY);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    if (length < 0) {
      throw new SQLException("Invalid length");
    }

    if (x == null && length != 0) {
      throw new SQLException("Invalid length");
    }

    set(parameterIndex, x, length, JDBCType.BINARY);
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

    set(parameterIndex, writer.toString(), JDBCType.VARCHAR);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), getJDBCType(x));
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), null, JDBCType.valueOf(targetSqlType));
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), scaleOrLength, JDBCType.valueOf(targetSqlType));
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), scaleOrLength, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapObject(connection, x), null, targetSqlType);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    checkClosed();

    set(parameterIndex, unwrapBlob(connection, x), JDBCType.BLOB);
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

    set(parameterIndex, blob, JDBCType.BLOB);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    set(parameterIndex, unwrapClob(connection, x), JDBCType.CLOB);
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

    set(parameterIndex, clob, JDBCType.CLOB);
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    set(parameterIndex, x, JDBCType.ARRAY);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    set(parameterIndex, null, JDBCType.valueOf(sqlType));
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    set(parameterIndex, x, JDBCType.VARCHAR);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    set(parameterIndex, unwrapXML(xmlObject), JDBCType.SQLXML);
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    set(parameterIndex, unwrapRowId(x), JDBCType.ROWID);
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
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
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
  public long executeLargeUpdate(String sql) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw NOT_ALLOWED_ON_PREP_STMT;
  }

}
