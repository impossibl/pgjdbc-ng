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

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;

import static com.impossibl.postgres.protocol.FieldFormats.REQUEST_ALL_TEXT;
import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;

import java.io.IOException;


/**
 * Generates a request to execute a prepared statement (usually "BEGIN").
 *
 * Commands are not sync'd and errors/notices are passed to the following
 * protocol handler. This allows it to prefix whatever command comes next
 * without waiting for completion.
 */
public class LazyExecuteRequest implements ServerRequest {

  private String txnStatementName;

  public LazyExecuteRequest(String txnStatementName) {
    this.txnStatementName = txnStatementName;
  }

  class Handler implements BindComplete, CommandComplete, CommandError, ReportNotice, ReadyForQuery {

    @Override
    public String toString() {
      return "Lazy Execute Statement";
    }

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) {
      return Action.Resume;
    }

    @Override
    public Action error(Notice notice) {
      // Pass error to next handler, cause it to believe an error occurred in its command
      return Action.ResumePassing;
    }

    @Override
    public Action notice(Notice notice) {
      // Pass any notices to the next handler, still need to wait for command complete.
      return Action.ResumePassing;
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) {
      return Action.Complete;
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

  @Override
  public Handler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel
        .writeBind(null, txnStatementName, EMPTY_FORMATS, EMPTY_BUFFERS, REQUEST_ALL_TEXT)
        .writeExecute(null, 0)
        .writeSync();

  }

}
