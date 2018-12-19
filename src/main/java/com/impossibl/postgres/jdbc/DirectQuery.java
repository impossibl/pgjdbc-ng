package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutorHandlers;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.CompositeQueryResults;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResult;
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


public class DirectQuery implements Query {

  private String sql;
  private FieldFormatRef[] parameterFormats;
  private ByteBuf[] parameterBuffers;
  private FieldFormatRef[] resultFieldFormats;
  private String portalName;
  private Status status;
  private Long timeout;
  private Integer maxRows;
  private List<ResultBatch> resultBatches;
  private ResultField[] suspendedResultFields;

  DirectQuery(String sql, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats) {
    this.sql = sql;
    this.parameterFormats = parameterFormats;
    this.parameterBuffers = parameterBuffers;
    this.resultFieldFormats = resultFieldFormats;
    this.status = Status.Initialized;
    this.resultBatches = new ArrayList<>();
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
    return resultBatches;
  }

  private boolean requiresPortal() {
    return maxRows != null;
  }

  private boolean hasParameters() {
    return parameterBuffers != null && parameterBuffers.length != 0;
  }

  private SQLWarning executeSimple(PGDirectConnection connection, String sql) throws SQLException {

    portalName = null;

    CompositeQueryResults results = connection.executeTimed(this.timeout, (timeout) ->{
      CompositeQueryResults handler = new CompositeQueryResults();
      connection.getRequestExecutor().query(sql, handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    resultBatches = results.getBatches();

    return chainWarnings(null, results);
  }

  private SQLWarning executeExtended(PGDirectConnection connection, String sql) throws SQLException {

    if (requiresPortal()) {
      portalName = connection.getNextPortalName();
    }
    else {
      portalName = null;
    }

    QueryResult result = connection.executeTimed(this.timeout, (timeout) ->{
      QueryResult handler = new QueryResult();
      connection.getRequestExecutor().query(sql, portalName, parameterFormats, parameterBuffers, resultFieldFormats, firstNonNull(maxRows, 0), handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    resultBatches = new ArrayList<>(singletonList(result.getBatch()));
    if (result.isSuspended()) {
      status = Status.Suspended;
      suspendedResultFields = resultBatches.get(0).getFields();
    }
    else if (portalName != null) {
      connection.execute((long timeout) -> connection.getRequestExecutor().close(ServerObjectType.Portal, portalName));
      portalName = null;
    }

    return chainWarnings(null, result);
  }

  private SQLWarning resumeExtended(PGDirectConnection connection) throws SQLException {

    RequestExecutorHandlers.ExecuteResult result = connection.executeTimed(this.timeout, (timeout) -> {
      RequestExecutorHandlers.ExecuteResult handler = new RequestExecutorHandlers.ExecuteResult(suspendedResultFields);
      connection.getRequestExecutor().resume(portalName, firstNonNull(maxRows, 0), handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    status = result.isSuspended() ? Status.Suspended : Status.Completed;
    resultBatches = new ArrayList<>(singletonList(result.getBatch()));

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
        if (suspendedResultFields == null) {
          throw new PGSQLSimpleException("Illegal query state - suspended with no previous results");
        }

        return resumeExtended(connection);
      }


      if (requiresPortal() || hasParameters()) {
        return executeExtended(connection, sql);
      }
      else {
        return executeSimple(connection, sql);
      }

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
    return "DirectQuery{" +
        "sql='" + sql + '\'' +
        ", portalName='" + portalName + '\'' +
        ", status=" + status +
        '}';
  }

}
