package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;

import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface Query {

  enum Status {
    Initialized,
    InProgress,
    Completed,
    Suspended,
  }

  boolean hasPortal();
  String getPortalName();

  Status getStatus();

  Long getTimeout();
  void setTimeout(Long timeout);

  Integer getMaxRows();
  void setMaxRows(Integer maxRows);

  List<ResultBatch> getResultBatches();

  SQLWarning execute(PGDirectConnection connection) throws SQLException;

  void dispose(PGDirectConnection connection) throws SQLException;

  static Query create(String sqlText) {
    return new DirectQuery(sqlText);
  }

  static Query create(String statement, ResultField[] resultFields) {
    return new PreparedQuery(statement, EMPTY_FORMATS, EMPTY_BUFFERS, resultFields);
  }

  static Query create(String statement, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, ResultField[] resultFields) {
    return new PreparedQuery(statement, parameterFormats, parameterBuffers, resultFields);
  }

}
