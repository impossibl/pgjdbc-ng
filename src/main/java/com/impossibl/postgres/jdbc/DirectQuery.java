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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutorHandlers;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.CompositeQueryResults;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;

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
  private int maxRows;
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
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return resultBatches;
  }

  private boolean requiresPortal() {
    return maxRows > 0;
  }

  private boolean hasParameters() {
    return parameterBuffers != null && parameterBuffers.length != 0;
  }

  private SQLWarning executeSimple(PGDirectConnection connection, String sql) throws SQLException {

    portalName = null;

    CompositeQueryResults results = connection.executeTimed(this.timeout, (timeout) -> {
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

    QueryResult result = connection.executeTimed(this.timeout, (timeout) -> {
      QueryResult handler = new QueryResult(!requiresPortal());
      connection.getRequestExecutor().query(sql, portalName, parameterFormats, parameterBuffers, resultFieldFormats, maxRows, handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    if (result.isSuspended()) {
      suspendedResultFields = result.getBatch().getFields();
    }
    return applyExecuteResult(connection, result);
  }

  private SQLWarning resumeExtended(PGDirectConnection connection) throws SQLException {

    ExecuteResult result = connection.executeTimed(this.timeout, (timeout) -> {
      ExecuteResult handler = new ExecuteResult(false, suspendedResultFields);
      connection.getRequestExecutor().resume(portalName, firstNonNull(maxRows, 0), handler);
      handler.await(timeout, MILLISECONDS);
      return handler;
    });

    status = result.isSuspended() ? Status.Suspended : Status.Completed;
    resultBatches = new ArrayList<>(singletonList(result.getBatch()));

    return chainWarnings(null, result);
  }

  private SQLWarning applyExecuteResult(PGDirectConnection connection, QueryResult result) throws SQLException {
    resultBatches = new ArrayList<>(singletonList(result.getBatch()));
    if (result.isSuspended()) {
      status = Status.Suspended;
    }
    else if (portalName != null) {
      dispose(connection);
    }

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
      connection.execute((timeout) -> {
        RequestExecutorHandlers.SynchronizedResult finish = new RequestExecutorHandlers.SynchronizedResult();
        connection.getRequestExecutor().finish(portalName, finish);
        finish.await(timeout, MILLISECONDS);
      });
    }

    portalName = null;
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
