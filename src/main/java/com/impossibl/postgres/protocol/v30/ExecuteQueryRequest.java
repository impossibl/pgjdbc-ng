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

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.RequestExecutor.QueryHandler;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NoData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterDescriptions;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.RowDescription;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.TypeRef;

import static com.impossibl.postgres.protocol.FieldFormats.REQUEST_ALL_BINARY;
import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.min;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;


class ExecuteQueryRequest implements ServerRequest {

  private String sql;
  private String portalName;
  private FieldFormatRef[] parameterFormatRefs;
  private ByteBuf[] parameterBuffers;
  private FieldFormatRef[] resultFieldFormatRefs;
  private int maxRows;
  private QueryHandler handler;
  private TypeRef[] describedParameterTypes;
  private ResultField[] describedResultFields;
  private List<RowData> rows;
  private List<Notice> notices;

  ExecuteQueryRequest(String sql, String portalName,
                      FieldFormatRef[] parameterFormatRefs,
                      ByteBuf[] parameterBuffers,
                      FieldFormatRef[] resultFieldFormatRefs,
                      QueryHandler handler) {
    this.sql = sql;
    this.portalName = portalName;
    this.parameterFormatRefs = parameterFormatRefs;
    this.parameterBuffers = parameterBuffers;
    this.resultFieldFormatRefs = resultFieldFormatRefs;
    this.maxRows = 0;
    this.handler = handler;
    this.describedParameterTypes = EMPTY_TYPES;
    this.describedResultFields = EMPTY_FIELDS;
    this.rows = new ArrayList<>();
    this.notices = new ArrayList<>();
  }

  private class Handler implements ParameterDescriptions, RowDescription, ParseComplete, BindComplete, NoData, DataRow, EmptyQuery, PortalSuspended, CommandComplete, CommandError {

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action parameterDescriptions(TypeRef[] types) {
      describedParameterTypes = types;
      return Action.Resume;
    }

    @Override
    public Action rowDescription(ResultField[] fields) {
      describedResultFields = fields;

      // Fix formats to match what was sent in the execute request

      FieldFormatRef[] effectiveFormats =
          resultFieldFormatRefs == null || resultFieldFormatRefs.length == 0 ? REQUEST_ALL_BINARY : resultFieldFormatRefs;

      for (int idx = 0; idx < describedResultFields.length; ++idx) {
        FieldFormat format = effectiveFormats[min(idx, effectiveFormats.length - 1)].getFormat();
        describedResultFields[idx].setFormat(format);
      }


      return Action.Resume;
    }

    @Override
    public Action parseComplete() {
      return Action.Resume;
    }

    @Override
    public Action rowData(ByteBuf data) {
      rows.add(new BufferRowData(data.retain()));
      return Action.Resume;
    }

    @Override
    public Action noData() {
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() throws IOException {

      try {
        handler.handleSuspend(describedParameterTypes, describedResultFields, rows, notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
    }

    @Override
    public Action emptyQuery() throws IOException {
      return commandComplete(null, null, null);
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException {

      try {
        handler.handleComplete(command, rowsAffected, insertedOid, describedParameterTypes, describedResultFields, rows, notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
    }

    @Override
    public Action error(Notice error) throws IOException {

      try {
        handler.handleError(new NoticeException(error), notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
    }

    @Override
    public void exception(Throwable cause) throws IOException {

      try {
        handler.handleError(cause, notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }
    }
  }

  @Override
  public ProtocolHandler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel
        .writeParse(null, sql, EMPTY_TYPES)
        .writeDescribe(Statement, null)
        .writeBind(portalName, null, parameterFormatRefs, parameterBuffers, resultFieldFormatRefs)
        .writeExecute(portalName, maxRows);

    if (maxRows > 0) {
      channel.writeFlush();
    }
    else {
      channel.writeSync();
    }

    channel.flush();

  }

}
