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

import com.impossibl.postgres.mapper.Mapper;
import com.impossibl.postgres.mapper.PropertySetter;
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.SettingsContext;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.StreamingByteBuf;
import com.impossibl.postgres.utils.guava.ByteStreams;

import static com.impossibl.postgres.protocol.ServerObjectType.Portal;
import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.system.Settings.PARAMETER_STREAM_THRESHOLD;
import static com.impossibl.postgres.system.Settings.PARAMETER_STREAM_THRESHOLD_DEFAULT;
import static com.impossibl.postgres.utils.Factory.createInstance;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

import io.netty.buffer.ByteBuf;

public class BindExecCommandImpl extends CommandImpl implements BindExecCommand {

  private static final int DEFAULT_MESSAGE_SIZE = 8192;
  private static final int STREAM_MESSAGE_SIZE = 32 * 1024;

  class BindExecCommandListener extends BaseProtocolListener {

    Context context;

    public BindExecCommandListener(Context context) {
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
      resultBatch.fields = newResultFields;
      resultBatch.results = !resultFields.isEmpty() ? new ArrayList<>() : null;
      resultSetters = Mapper.buildMapping(rowType, newResultFields);
    }

    @Override
    public void noData() {
      resultBatch.fields = Collections.emptyList();
      resultBatch.results = null;
      status = Status.Completed;
    }

    @Override
    public void rowData(ByteBuf buffer) throws IOException {

      int itemCount = buffer.readShort();

      Object rowInstance = createInstance(rowType, itemCount);

      for (int c = 0; c < itemCount; ++c) {

        ResultField field = resultBatch.fields.get(c);

        Type fieldType = field.typeRef.get();

        Type.Codec.Decoder decoder = fieldType.getCodec(field.format).decoder;

        Object fieldVal = decoder.decode(fieldType, field.typeLength, field.typeModifier, buffer, context);

        resultSetters.get(c).set(rowInstance, fieldVal);
      }

      @SuppressWarnings("unchecked")
      List<Object> res = (List<Object>) resultBatch.results;
      res.add(rowInstance);

    }

    @Override
    public void emptyQuery() {
      resultBatch.fields = Collections.emptyList();
      resultBatch.results = null;
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
      resultBatch.command = command;
      resultBatch.rowsAffected = rowsAffected;
      resultBatch.insertedOid = oid;

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
  private Class<?> rowType;
  private List<PropertySetter> resultSetters;
  private int maxRows;
  private int maxFieldLength;
  private Status status;
  private SettingsContext parsingContext;
  private ResultBatch resultBatch;
  private List<Format> resultFieldFormats;
  private long queryTimeout;


  public BindExecCommandImpl(String portalName, String statementName, List<Type> parameterTypes, List<Object> parameterValues, List<ResultField> resultFields, Class<?> rowType) {

    this.statementName = statementName;
    this.portalName = portalName;
    this.parameterTypes = parameterTypes;
    this.parameterValues = parameterValues;
    this.resultFields = resultFields;
    this.rowType = rowType;
    this.maxRows = 0;
    this.maxFieldLength = Integer.MAX_VALUE;

    if (resultFields != null) {
      this.resultSetters = Mapper.buildMapping(rowType, resultFields);
      this.resultFieldFormats = getResultFieldFormats(resultFields);
    }
    else {
      this.resultSetters = Collections.emptyList();
      this.resultFieldFormats = Collections.emptyList();
    }

  }

  public void reset() {
    status = null;
    resultBatch = new ResultBatch();
    resultBatch.fields = resultFields;
    resultBatch.results = (resultFields != null && !resultFields.isEmpty()) ? new ArrayList<>() : null;
  }

  @Override
  public long getQueryTimeout() {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(long queryTimeout) {
    this.queryTimeout = queryTimeout;
  }

  @Override
  public String getStatementName() {
    return statementName;
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
  public List<Object> getParameterValues() {
    return parameterValues;
  }

  @Override
  public void setParameterValues(List<Object> parameterValues) {
    this.parameterValues = parameterValues;
  }

  @Override
  public int getMaxRows() {
    return maxRows;
  }

  @Override
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @Override
  public int getMaxFieldLength() {
    return maxFieldLength;
  }

  @Override
  public void setMaxFieldLength(int maxFieldLength) {
    this.maxFieldLength = maxFieldLength;
  }

  @Override
  public List<ResultBatch> getResultBatches() {
    return asList(resultBatch);
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    // Setup context for parsing fields with customized parameters
    //
    parsingContext = new SettingsContext(protocol.getContext());
    parsingContext.setSetting(FIELD_VARYING_LENGTH_MAX, maxFieldLength);

    BindExecCommandListener listener = new BindExecCommandListener(parsingContext);

    protocol.setListener(listener);

    ByteBuf msg = protocol.channel.alloc().buffer(DEFAULT_MESSAGE_SIZE);

    if (status != Status.Suspended) {

      if (shouldStreamBind(parsingContext, parameterValues)) {

        StreamingByteBuf bindMsg = new StreamingByteBuf(protocol.channel, STREAM_MESSAGE_SIZE);

        protocol.writeBind(bindMsg, portalName, statementName, parameterTypes, parameterValues, resultFieldFormats, true);

        bindMsg.flush();

      }
      else {

        protocol.writeBind(msg, portalName, statementName, parameterTypes, parameterValues, resultFieldFormats, true);

      }

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

  static boolean shouldStreamBind(Context context, List<Object> parameterValues) {

    int streamThreshold = context.getSetting(PARAMETER_STREAM_THRESHOLD, PARAMETER_STREAM_THRESHOLD_DEFAULT);
    int streamTotal = 0;

    for (Object parameterValue : parameterValues) {

      if (parameterValue instanceof ByteStreams.LimitedInputStream) {
        streamTotal += ((ByteStreams.LimitedInputStream) parameterValue).limit();
      }
      else if (parameterValue instanceof InputStream) {
        return false;
      }
    }

    return streamTotal > streamThreshold;
  }

  static List<Format> getResultFieldFormats(List<ResultField> resultFields) {

    List<Format> resultFieldFormats = new ArrayList<>();

    for (ResultField resultField : resultFields) {
      resultField.format = resultField.typeRef.get().getResultFormat();
      resultFieldFormats.add(resultField.format);
    }

    return resultFieldFormats;
  }

}
