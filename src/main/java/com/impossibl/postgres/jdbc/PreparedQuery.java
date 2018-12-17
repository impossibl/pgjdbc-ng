package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResults;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.buffer.ByteBuf;


public class PreparedQuery implements Query {

  private String statementName;
  private FieldFormatRef[] parameterFormatRefs;
  private ByteBuf[] parameterBuffers;
  private ResultField[] resultFields;
  private String portalName;
  private Status status;
  private Long timeout;
  private Integer maxRows;
  private ResultBatch resultBatch;

  PreparedQuery(String statementName, FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers, ResultField[] resultFields) {
    this.statementName = statementName;
    this.parameterFormatRefs = parameterFormatRefs;
    this.parameterBuffers = parameterBuffers;
    this.resultFields = resultFields;
    this.status = Status.Initialized;
  }

  @Override
  public boolean hasPortal() {
    return portalName != null;
  }

  @Override
  public String getPortalName() {
    return portalName;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public Long getTimeout() {
    return timeout;
  }

  @Override
  public void setTimeout(Long timeout) {
    this.timeout = timeout;
  }

  @Override
  public Integer getMaxRows() {
    return maxRows;
  }

  @Override
  public void setMaxRows(Integer maxRows) {
    this.maxRows = maxRows;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return new ArrayList<>(singletonList(resultBatch));
  }

  @Override
  public SQLWarning execute(PGDirectConnection connection) throws SQLException {

    if (status == Status.Suspended) {

      if (portalName != null) {
        throw new PGSQLSimpleException("Illegal query state - suspended with no portal");
      }

      ExecuteResults results = connection.executeTimed(this.timeout, (timeout) -> {
        ExecuteResults handler = new ExecuteResults(resultFields);
        connection.getRequestExecutor().resume(portalName, maxRows, handler);
        handler.await(timeout, MILLISECONDS);
        return handler;
      });

      return chainWarnings(null, results);
    }

    if (maxRows != null) {
      portalName = connection.getNextPortalName();
    }

    status = Status.InProgress;
    try {

      ExecuteResults results = connection.executeTimed(this.timeout, (timeout) -> {
        ExecuteResults handler = new ExecuteResults(resultFields);
        connection.getRequestExecutor().execute(portalName, statementName, parameterFormatRefs, parameterBuffers, resultFields, maxRows, handler);
        handler.await(timeout, MILLISECONDS);
        return handler;
      });

      resultBatch = results.getResultBatch();

      return chainWarnings(null, results);
    }
    finally {
      status = Status.Completed;
    }

  }

  @Override
  public void dispose(PGDirectConnection connection) throws SQLException {

    if (portalName != null) {
      connection.execute((long timeout) -> connection.getRequestExecutor().close(ServerObjectType.Portal, portalName));
    }

  }

}
