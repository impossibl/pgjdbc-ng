package com.impossibl.postgres.jdbc;

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


public class DirectQuery implements Query {

  private String sql;
  private String portalName;
  private Status status;
  private Long timeout;
  private Integer maxRows;
  private ResultBatch resultBatch;

  DirectQuery(String sql) {
    this.sql = sql;
    this.status = Status.Initialized;
  }

  @Override
  public boolean hasPortal() {
    return false;
  }

  @Override
  public String getPortalName() {
    return null;
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

    if (portalName == null) {
      portalName = connection.getNextPortalName();
    }

    return connection.executeTimed(this.timeout, (timeout) ->{
      QueryResults results = new QueryResults();
      connection.getRequestExecutor().query(sql, portalName, results);
      results.await(timeout, MILLISECONDS);
      return results;
    });

  }

  @Override
  public SQLWarning execute(PGDirectConnection connection) throws SQLException {

    status = Status.InProgress;
    try {

      QueryResults results;

      if (maxRows != null) {
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
