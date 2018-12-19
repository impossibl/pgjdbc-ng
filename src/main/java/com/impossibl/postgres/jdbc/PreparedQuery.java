package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerObjectType;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

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
  public void setMaxRows(Integer maxRows) {
    this.maxRows = maxRows != null && maxRows > 0 ? maxRows : null;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return new ArrayList<>(singletonList(resultBatch));
  }

  private boolean requiresPortal() {
    return maxRows != null;
  }

  private SQLWarning executeStatement(PGDirectConnection connection) throws SQLException {

    if (requiresPortal()) {
      portalName = connection.getNextPortalName();
    }
    else {
      portalName = null;
    }

    ExecuteResult result = connection.executeTimed(this.timeout, (timeout) -> {
      ExecuteResult handler = new ExecuteResult(resultFields);
      connection.getRequestExecutor().execute(portalName, statementName, parameterFormatRefs, parameterBuffers, resultFields, firstNonNull(maxRows, 0), handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    resultBatch = result.getBatch();
    if (result.isSuspended()) {
      status = Status.Suspended;
    }
    else if (portalName != null) {
      connection.execute((long timeout) -> connection.getRequestExecutor().close(ServerObjectType.Portal, portalName));
      portalName = null;
    }

    return chainWarnings(null, result);
  }

  private SQLWarning resumeStatement(PGDirectConnection connection) throws SQLException {

    ExecuteResult result = connection.executeTimed(this.timeout, (timeout) -> {
      ExecuteResult handler = new ExecuteResult(resultFields);
      connection.getRequestExecutor().resume(portalName, firstNonNull(maxRows, 0), handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    status = result.isSuspended() ? Status.Suspended : Status.Completed;
    resultBatch = result.getBatch();

    return chainWarnings(null, result);
  }

  @Override
  public SQLWarning execute(PGDirectConnection connection) throws SQLException {

    boolean wasSuspended = status == Status.Suspended;

    status = Status.InProgress;
    try {

      if (wasSuspended) {

        if (portalName == null) {
          throw new PGSQLSimpleException("Illegal query state - suspended with no portal");
        }

        return resumeStatement(connection);
      }

      return executeStatement(connection);
    }
    finally {
      if (status == Status.InProgress) {
        status = Status.Completed;
      }
    }

  }

  @Override
  public void dispose(PGDirectConnection connection) throws SQLException {

    if (portalName != null) {
      connection.execute((long timeout) -> connection.getRequestExecutor().close(ServerObjectType.Portal, portalName));
    }

  }

  @Override
  public String toString() {
    return "PreparedQuery{" +
        "statementName='" + statementName + '\'' +
        ", portalName='" + portalName + '\'' +
        ", status=" + status +
        '}';
  }

}
