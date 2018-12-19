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
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.BufferRowData;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareExecCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;

import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;


class PrepareExecCommandImpl extends CommandImpl implements PrepareExecCommand {

  private static final int DEFAULT_MESSAGE_SIZE = 512;

  private class TxnListener extends BaseProtocolListener {
  }

  private class QueryListener extends BaseProtocolListener {

    @Override
    public void rowDescription(ResultField[] newResultFields) {
      resultFields = newResultFields;
      resultFieldFormats = getResultFieldFormats(newResultFields);
      resultBatch.setFields(newResultFields);
      resultBatch.resetResults(true);
    }

    @Override
    public void noData() {
      resultBatch.setFields(EMPTY_FIELDS);
      resultBatch.resetResults(false);
    }

    @Override
    public void rowData(ByteBuf buffer) {
      resultBatch.addResult(BufferRowData.parseFields(buffer.retain(), resultFields));
    }

    @Override
    public void emptyQuery() {
      resultBatch.setFields(EMPTY_FIELDS);
      resultBatch.resetResults(false);
      status = Status.Completed;
    }

    @Override
    public void portalSuspended() {
      status = Status.Suspended;
    }

    @Override
    public void commandComplete(String command, Long rowsAffected, Long oid) {
      status = Status.Completed;
      resultBatch.setCommand(command);
      resultBatch.setRowsAffected(rowsAffected);
      resultBatch.setInsertedOid(oid);
    }

  }

  class TxnQueryListener extends DelegatedProtocolListener {

    QueryListener queryListener;

    TxnQueryListener(TxnListener txnListener, QueryListener queryListener) {
      super(txnListener);
      this.queryListener = queryListener;
    }

    TxnQueryListener(QueryListener queryListener) {
      super(queryListener);
      this.queryListener = queryListener;
    }

    boolean isExpectingTransaction() {
      return delegate != queryListener;
    }

    @Override
    public boolean isComplete() {
      return status != null || error != null || exception != null;
    }

    @Override
    public void notice(Notice notice) {
      addNotice(notice);
    }

    @Override
    public void commandComplete(String command, Long rowsAffected, Long oid) throws IOException {
      super.commandComplete(command, rowsAffected, oid);
      delegate = queryListener;
      notifyPossibleCompletion();
    }

    @Override
    public void portalSuspended() throws IOException {
      super.portalSuspended();
      notifyPossibleCompletion();
    }

    @Override
    public void ready(TransactionStatus txStatus) throws IOException {
      super.ready(txStatus);
      notifyPossibleCompletion();
    }

    @Override
    public void error(Notice error) {
      PrepareExecCommandImpl.this.error = error;
      notifyPossibleCompletion();
    }

    @Override
    public void exception(Throwable cause) {
      setException(cause);
      notifyPossibleCompletion();
    }

  }


  private String sql;
  private String portalName;
  private ResultField[] resultFields;
  private int maxRows;
  private Status status;
  private ResultBatch resultBatch;
  private FieldFormat[] resultFieldFormats;
  private long queryTimeout;
  private boolean requireActiveTransaction;


  PrepareExecCommandImpl(String sql, String portalName, ResultField[] resultFields) {

    this.sql = sql;
    this.portalName = portalName;
    this.resultFields = resultFields;
    this.maxRows = 0;

    if (resultFields != null) {
      this.resultFieldFormats = getResultFieldFormats(resultFields);
    }
    else {
      this.resultFieldFormats = EMPTY_FORMATS;
    }

    this.requireActiveTransaction = false;
  }

  private void reset() {
    status = null;
    resultBatch = new ResultBatch();
    resultBatch.setFields(resultFields);
    resultBatch.resetResults(true);
    requireActiveTransaction = false;
  }

  @Override
  public void setRequireActiveTransaction(boolean transactionRequired) {
    this.requireActiveTransaction = transactionRequired;
  }

  @Override
  public void setQueryTimeout(long queryTimeout) {
    this.queryTimeout = queryTimeout;
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
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return new ArrayList<>(singletonList(resultBatch));
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    // Setup context for parsing fields with customized parameters
    //

    TxnQueryListener listener;
    if (requireActiveTransaction && status != Status.Suspended && protocol.getTransactionStatus() == TransactionStatus.Idle) {
      listener = new TxnQueryListener(new TxnListener(), new QueryListener());
    }
    else {
      listener = new TxnQueryListener(new QueryListener());
    }

    protocol.setListener(listener);

    ByteBuf msg = protocol.getChannel().alloc().buffer(DEFAULT_MESSAGE_SIZE);
    try {

      if (listener.isExpectingTransaction()) {

        protocol.writeBind(msg, null, "TB", EMPTY_FORMATS, EMPTY_BUFFERS, null);
        protocol.writeExecute(msg, null, 0);

      }

      if (status != Status.Suspended) {

        protocol.writeParse(msg, null, sql, EMPTY_TYPES);

        protocol.writeDescribe(msg, Statement, null);

        protocol.writeBind(msg, portalName, null, EMPTY_FORMATS, EMPTY_BUFFERS, resultFieldFormats);
      }

      reset();

      protocol.writeExecute(msg, portalName, maxRows);

      if (maxRows > 0 && protocol.getTransactionStatus() == TransactionStatus.Idle) {
        protocol.writeFlush(msg);
      }
      else {
        protocol.writeSync(msg);
      }

    }
    catch (Throwable t) {
      msg.release();
      throw t;
    }

    protocol.send(msg);

    enableCancelTimer(protocol, queryTimeout);

    listener.waitUntilComplete(networkTimeout);

    if (ResourceLeakDetector.getLevel().compareTo(ResourceLeakDetector.Level.SIMPLE) > 0) {
      // Touch results batches (and therefore DataRows) in the
      // correct thread to make debugging leaks easier.
      if (resultBatch != null) {
        resultBatch.touch("Bind/Exec Completed");
      }
    }
  }

  private static FieldFormat[] getResultFieldFormats(ResultField[] resultFields) {

    FieldFormat[] resultFieldFormats = new FieldFormat[resultFields.length];

    for (int idx = 0; idx < resultFieldFormats.length; ++idx) {
      ResultField resultField = resultFields[idx];
      resultField.setFormat(resultField.getTypeRef().get().getResultFormat());
      resultFieldFormats[idx] = resultField.getFormat();
    }

    return resultFieldFormats;
  }

}
