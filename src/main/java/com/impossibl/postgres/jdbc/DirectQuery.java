package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResults;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ServerObjectType;

import static com.impossibl.postgres.jdbc.ErrorUtils.chainWarnings;

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
  private String portalName;
  private Status status;
  private Long timeout;
  private Integer maxRows;
  private ResultBatch resultBatch;

  DirectQuery(String sql, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers) {
    this.sql = sql;
    this.parameterFormats = parameterFormats;
    this.parameterBuffers = parameterBuffers;
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
    this.maxRows = maxRows;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return new ArrayList<>(singletonList(resultBatch));
  }

  private boolean requiresPortal() {
    return maxRows != null;
  }

  private boolean hasParameters() {
    return parameterBuffers.length != 0;
  }

  private QueryResults executeSimple(PGDirectConnection connection, String sql) throws SQLException {

    portalName = null;

    return connection.executeTimed(this.timeout, (timeout) ->{
      QueryResults results = new QueryResults();
      connection.getRequestExecutor().query(sql, results);
      results.await(timeout, MILLISECONDS);
      return results;
    });

  }

  private QueryResults executeExtended(PGDirectConnection connection, String sql) throws SQLException {

    if (requiresPortal()) {
      portalName = connection.getNextPortalName();
    }
    else {
      portalName = null;
    }

    return connection.executeTimed(this.timeout, (timeout) ->{
      QueryResults results = new QueryResults();
      connection.getRequestExecutor().query(sql, portalName, parameterFormats, parameterBuffers, results);
      results.await(timeout, MILLISECONDS);
      return results;
    });

  }

  @Override
  public SQLWarning execute(PGDirectConnection connection) throws SQLException {

    status = Status.InProgress;
    try {

      QueryResults results;

      if (requiresPortal() || hasParameters()) {
        results = executeExtended(connection, sql);
      }
      else {
        results = executeSimple(connection, sql);
      }

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
