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

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.RequestExecutor.ExecuteHandler;
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

import static io.netty.util.ReferenceCountUtil.release;


public class ExecuteStatementRequest implements ServerRequest {

  private String statementName;
  private String portalName;
  private FieldFormatRef[] parameterFormats;
  private ByteBuf[] parameterBuffers;
  private FieldFormatRef[] resultFieldFormats;
  private int maxRows;
  private ExecuteHandler handler;
  private RowDataSet rows;
  private List<Notice> notices;

  ExecuteStatementRequest(String statementName, String portalName,
                          FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers,
                          FieldFormatRef[] resultFieldFormats, int maxRows,
                          ExecuteHandler handler) {
    this.statementName = statementName;
    this.portalName = portalName;
    this.parameterFormats = parameterFormats;
    this.parameterBuffers = parameterBuffers;
    this.resultFieldFormats = resultFieldFormats;
    this.maxRows = maxRows;
    this.handler = handler;
    this.rows = new RowDataSet();
    this.notices = new ArrayList<>();
  }

  private boolean isSynchronized() {
    return maxRows == 0;
  }

  private class Handler implements BindComplete, DataRow, EmptyQuery, PortalSuspended, CommandComplete, ReportNotice, CommandError, ReadyForQuery {

    @Override
    public String toString() {
      return "Execute Statement";
    }

    @Override
    public Action notice(Notice notice) {
      notices.add(notice);
      return Action.Resume;
    }

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action rowData(ByteBuf data) {
      rows.add(new BufferRowData(data.retain()));
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() throws IOException {

      try {
        handler.handleSuspend(rows, notices);
      }
      finally {
        release(rows);
      }

      return Action.Complete;
    }

    @Override
    public Action emptyQuery() throws IOException {
      return commandComplete(null, null, null);
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException {

      try {
        handler.handleComplete(command, rowsAffected, insertedOid, rows, notices);
      }
      finally {
        release(rows);
      }

      return isSynchronized() ? Action.Resume : Action.Complete;
    }

    @Override
    public Action error(Notice error) throws IOException {

      try {
        handler.handleError(new NoticeException(error), notices);
      }
      finally {
        release(rows);
      }

      return isSynchronized() ? Action.Resume : Action.Complete;
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) throws IOException {
      handler.handleReady(txnStatus);
      return Action.Complete;
    }

    @Override
    public void exception(Throwable cause) throws IOException {

      try {
        handler.handleError(cause, notices);
      }
      finally {
        release(rows);
      }

    }

  }

  @Override
  public ProtocolHandler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel.writeBind(portalName, statementName, parameterFormats, parameterBuffers, resultFieldFormats);
    channel.writeExecute(portalName, maxRows);

    if (!isSynchronized()) {
      channel.writeFlush();
    }
    else {
      channel.writeSync();
    }

    channel.flush();
  }

}
