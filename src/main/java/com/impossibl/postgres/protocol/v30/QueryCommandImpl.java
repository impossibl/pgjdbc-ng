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
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;

import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;

class QueryCommandImpl extends CommandImpl implements QueryCommand {

  private class TxnListener extends BaseProtocolListener {
  }

  private class QueryListener extends BaseProtocolListener {

    @Override
    public boolean isComplete() {
      return !resultBatches.isEmpty() || error != null || exception != null;
    }

    @Override
    public void noData() {
      resultBatch.setFields(EMPTY_FIELDS);
      resultBatch.resetResults(false);
    }

    @Override
    public void rowDescription(ResultField[] resultFields) {
      resultBatch.setFields(resultFields);
      resultBatch.resetResults(true);
    }

    @Override
    public void rowData(ByteBuf buffer) {
      resultBatch.getResults().add(BufferRowData.parseFields(buffer.retain(), resultBatch.getFields()));
    }

    @Override
    public void emptyQuery() {
      resultBatch.setFields(EMPTY_FIELDS);
      resultBatch.resetResults(false);

      resultBatches.add(resultBatch);
      resultBatch = new ResultBatch();
    }

    @Override
    public void commandComplete(String command, Long rowsAffected, Long oid) {
      resultBatch.setCommand(command);
      resultBatch.setRowsAffected(rowsAffected);
      resultBatch.setInsertedOid(oid);

      resultBatches.add(resultBatch);
      resultBatch = new ResultBatch();
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
      return !resultBatches.isEmpty() || error != null || exception != null;
    }

    @Override
    public void notice(Notice notice) {
      addNotice(notice);
    }

    @Override
    public void commandComplete(String command, Long rowsAffected, Long oid) throws IOException {
      super.commandComplete(command, rowsAffected, oid);
      delegate = queryListener;
    }

    @Override
    public void ready(TransactionStatus txStatus) throws IOException {
      super.ready(txStatus);
      notifyPossibleCompletion();
    }

    @Override
    public void error(Notice error) {
      QueryCommandImpl.this.error = error;
      notifyPossibleCompletion();
    }

    @Override
    public void exception(Throwable cause) {
      setException(cause);
      notifyPossibleCompletion();
    }

  }


  private String command;
  private List<ResultBatch> resultBatches;
  private ResultBatch resultBatch;
  private long queryTimeout;
  private boolean requireActiveTransaction;


  QueryCommandImpl(String command) {
    this.command = command;
    this.requireActiveTransaction = false;
  }

  @Override
  public void setRequireActiveTransaction(boolean requireActiveTransaction) {
    this.requireActiveTransaction = requireActiveTransaction;
  }

  @Override
  public void setQueryTimeout(long timeout) {
    this.queryTimeout = timeout;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return resultBatches;
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    resultBatch = new ResultBatch();
    resultBatches = new ArrayList<>();

    TxnQueryListener listener;
    if (requireActiveTransaction && protocol.getTransactionStatus() == TransactionStatus.Idle) {
      listener = new TxnQueryListener(new TxnListener(), new QueryListener());
    }
    else {
      listener = new TxnQueryListener(new QueryListener());
    }

    protocol.setListener(listener);

    ByteBuf msg = protocol.getChannel().alloc().buffer();
    try {

      if (listener.isExpectingTransaction()) {

        protocol.writeBind(msg, null, "TB", EMPTY_FORMATS, EMPTY_BUFFERS, EMPTY_FORMATS);
        protocol.writeExecute(msg, null, 0);
      }

      protocol.writeQuery(msg, command);
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
      if (resultBatches != null) {
        for (ResultBatch resultBatch : resultBatches) {
          resultBatch.touch("Query Completed");
        }
      }
    }
  }

  @Override
  public Status getStatus() {
    return Status.Completed;
  }

  @Override
  public void setMaxRows(int maxRows) {
  }

}
