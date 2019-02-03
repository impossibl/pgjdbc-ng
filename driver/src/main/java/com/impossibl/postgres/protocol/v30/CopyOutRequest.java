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

import com.impossibl.postgres.protocol.CopyFormat;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.RequestExecutor.CopyOutHandler;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyDone;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyFail;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyOutResponse;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.RowDescription;
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

public class CopyOutRequest implements ServerRequest {

  private String sql;
  private OutputStream stream;
  private CopyOutHandler handler;
  private List<Notice> notices;

  CopyOutRequest(String sql, OutputStream stream, CopyOutHandler handler) {
    this.sql = sql;
    this.stream = stream;
    this.handler = handler;
    this.notices = new ArrayList<>();
  }

  private class Handler implements CopyOutResponse, CopyData, CopyDone, CopyFail, RowDescription, EmptyQuery, CommandComplete, CommandError, ReportNotice, ReadyForQuery {

    boolean started = false;

    @Override
    public ProtocolHandler copyOut(CopyFormat format, FieldFormat[] columnFormats) {
      started = true;
      return this;
    }

    @Override
    public void copyData(ByteBuf data) throws IOException {
      data.readBytes(stream, data.readableBytes());
    }

    @Override
    public void copyDone() {
    }

    @Override
    public void copyFail(String message) {
      notices.add(new Notice("", "", message));
    }

    @Override
    public Action rowDescription(ResultField[] fields) {
      return Action.Resume;
    }

    @Override
    public Action emptyQuery() {
      return Action.Resume;
    }

    @Override
    public Action notice(Notice notice) {
      notices.add(notice);
      return Action.Resume;
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException {
      if (!started) {
        handler.handleError(new IOException("Command Not Initiated: COPY OUT"), notices);
      }
      else {
        handler.handleComplete();
      }
      return Action.Resume;
    }

    @Override
    public Action error(Notice notice) throws IOException {
      handler.handleError(new NoticeException(notice), notices);
      return Action.Resume;
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) throws IOException {
      handler.handleReady(txnStatus);
      return Action.Complete;
    }

    @Override
    public void exception(Throwable cause) throws IOException {
      handler.handleError(cause, notices);
    }

  }

  @Override
  public ProtocolHandler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel.writeQuery(sql).flush();

  }

}
