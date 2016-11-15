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

import com.impossibl.postgres.protocol.BufferedDataRow;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;

import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

class QueryCommandImpl extends CommandImpl implements QueryCommand {

  private class Listener extends BaseProtocolListener {

    Context context;

    Listener(Context context) {
      super();
      this.context = context;
    }

    @Override
    public boolean isComplete() {
      return !resultBatches.isEmpty() || error != null || exception != null;
    }

    @Override
    public void rowDescription(List<ResultField> resultFields) {
      resultBatch.setFields(resultFields);
      resultBatch.resetResults(true);
    }

    @Override
    public void rowData(ByteBuf buffer) throws IOException {
      resultBatch.addResult(BufferedDataRow.parse(buffer, resultBatch.getFields(), parsingContext));
    }

    @Override
    public void commandComplete(String command, Long rowsAffected, Long oid) {
      resultBatch.setCommand(command);
      resultBatch.setRowsAffected(rowsAffected);
      resultBatch.setInsertedOid(oid);

      resultBatches.add(resultBatch);
      resultBatch = new ResultBatch();
    }

    @Override
    public synchronized void error(Notice error) {
      QueryCommandImpl.this.error = error;
      notifyAll();
    }

    @Override
    public synchronized void exception(Throwable cause) {
      setException(cause);
      notifyAll();
    }

    @Override
    public void notice(Notice notice) {
      addNotice(notice);
    }

    @Override
    public synchronized void ready(TransactionStatus txStatus) {
      notifyAll();
    }

  }


  private String command;
  private List<ResultBatch> resultBatches;
  private ResultBatch resultBatch;
  private long queryTimeout;
  private SettingsContext parsingContext;
  private int maxFieldLength;


  QueryCommandImpl(String command) {
    this.command = command;
    this.maxFieldLength = Integer.MAX_VALUE;
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

    // Setup context for parsing fields with customized parameters
    //
    parsingContext = new SettingsContext(protocol.getContext());
    parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

    Listener listener = new Listener(protocol.getContext());

    protocol.setListener(listener);

    ByteBuf msg = protocol.channel.alloc().buffer();

    protocol.writeQuery(msg, command);

    protocol.writeSync(msg);

    protocol.send(msg);

    enableCancelTimer(protocol, queryTimeout);

    waitFor(listener);
  }

  @Override
  public Status getStatus() {
    return Status.Completed;
  }

  @Override
  public void setMaxFieldLength(int maxFieldLength) {
  }

  @Override
  public void setMaxRows(int maxRows) {
  }

}
