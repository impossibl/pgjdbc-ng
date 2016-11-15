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

import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.BufferedDataRow;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.ServerObjectType.Portal;
import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

import io.netty.buffer.ByteBuf;


class BindExecCommandImpl extends CommandImpl implements BindExecCommand {

  private static final int DEFAULT_MESSAGE_SIZE = 8192;

  private class Listener extends BaseProtocolListener {

    Context context;

    Listener(Context context) {
      this.context = context;
    }

    @Override
    public boolean isComplete() {
      return status != null || error != null || exception != null;
    }

    @Override
    public void bindComplete() {
    }

    @Override
    public void rowDescription(List<ResultField> newResultFields) {
      resultFields = newResultFields;
      resultFieldFormats = getResultFieldFormats(newResultFields);
      resultBatch.setFields(newResultFields);
      resultBatch.resetResults(true);
    }

    @Override
    public void noData() {
      resultBatch.setFields(Collections.<ResultField>emptyList());
      resultBatch.resetResults(false);
    }

    @Override
    public void rowData(ByteBuf buffer) throws IOException {
      resultBatch.addResult(BufferedDataRow.parse(buffer, resultFields, parsingContext));
    }

    @Override
    public void emptyQuery() {
      resultBatch.setFields(Collections.<ResultField>emptyList());
      resultBatch.resetResults(false);
      status = Status.Completed;
    }

    @Override
    public synchronized void portalSuspended() {
      status = Status.Suspended;
      notifyAll();
    }

    @Override
    public synchronized void commandComplete(String command, Long rowsAffected, Long oid) {
      status = Status.Completed;
      resultBatch.setCommand(command);
      resultBatch.setRowsAffected(rowsAffected);
      resultBatch.setInsertedOid(oid);

      if (maxRows > 0) {
        notifyAll();
      }
    }

    @Override
    public synchronized void error(Notice error) {
      BindExecCommandImpl.this.error = error;
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


  private String statementName;
  private String portalName;
  private List<Type> parameterTypes;
  private List<Object> parameterValues;
  private List<ResultField> resultFields;
  private int maxRows;
  private int maxFieldLength;
  private Status status;
  private ResultBatch resultBatch;
  private List<Format> resultFieldFormats;
  private SettingsContext parsingContext;
  private long queryTimeout;


  BindExecCommandImpl(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues,
                      List<ResultField> resultFields) {

    this.statementName = statementName;
    this.portalName = portalName;
    this.parameterTypes = parameterTypes;
    this.parameterValues = parameterValues;
    this.resultFields = resultFields;
    this.maxRows = 0;
    this.maxFieldLength = Integer.MAX_VALUE;

    if (resultFields != null) {
      this.resultFieldFormats = getResultFieldFormats(resultFields);
    }
    else {
      this.resultFieldFormats = Collections.emptyList();
    }

  }

  private void reset() {
    status = null;
    resultBatch = new ResultBatch();
    resultBatch.setFields(resultFields);
    resultBatch.resetResults(true);
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
  public List<Type> getParameterTypes() {
    return parameterTypes;
  }

  @Override
  public void setParameterTypes(List<Type> parameterTypes) {
    this.parameterTypes = parameterTypes;
  }

  @Override
  public void setParameterValues(List<Object> parameterValues) {
    this.parameterValues = parameterValues;
  }

  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @Override
  public void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return singletonList(resultBatch);
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    // Setup context for parsing fields with customized parameters
    //
    parsingContext = new SettingsContext(protocol.getContext());
    parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

    Listener listener = new Listener(parsingContext);

    protocol.setListener(listener);

    ByteBuf msg = protocol.channel.alloc().buffer(DEFAULT_MESSAGE_SIZE);

    if (status != Status.Suspended) {

      protocol.writeBind(msg, portalName, statementName, parameterTypes, parameterValues, resultFieldFormats);

    }

    reset();

    if (resultFields == null) {

      protocol.writeDescribe(msg, Portal, portalName);

    }

    protocol.writeExecute(msg, portalName, maxRows);

    if (maxRows > 0 && protocol.getTransactionStatus() == TransactionStatus.Idle) {
      protocol.writeFlush(msg);
    }
    else {
      protocol.writeSync(msg);
    }

    protocol.send(msg);

    enableCancelTimer(protocol, queryTimeout);

    waitFor(listener);

  }

  private static List<Format> getResultFieldFormats(List<ResultField> resultFields) {

    List<Format> resultFieldFormats = new ArrayList<>();

    for (ResultField resultField : resultFields) {
      resultField.setFormat(resultField.getTypeRef().get().getResultFormat());
      resultFieldFormats.add(resultField.getFormat());
    }

    return resultFieldFormats;
  }

}
