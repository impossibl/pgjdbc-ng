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
import com.impossibl.postgres.protocol.RequestExecutor.SynchronizedHandler;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CloseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;

import static java.util.Collections.emptyList;


public class CloseRequest implements ServerRequest {

  private ServerObjectType objectType;
  private String objectName;
  private SynchronizedHandler handler;

  CloseRequest(ServerObjectType objectType, String objectName, SynchronizedHandler handler) {
    this.objectType = objectType;
    this.objectName = objectName;
    this.handler = handler;
  }

  class LazyHandler implements CloseComplete, CommandError {

    @Override
    public String toString() {
      return "Close " + objectType;
    }

    @Override
    public Action closeComplete() {
      return Action.Complete;
    }

    @Override
    public Action error(Notice notice) {
      return Action.Complete;
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

  class SyncedHandler implements CloseComplete, CommandError, ReadyForQuery {

    @Override
    public String toString() {
      return "Close " + objectType + ": (Synchronized)";
    }

    @Override
    public Action closeComplete() {
      return Action.Resume;
    }

    @Override
    public Action error(Notice notice) throws IOException {
      handler.handleError(new NoticeException(notice), emptyList());
      return Action.Resume;
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) throws IOException {
      handler.handleReady(txnStatus);
      return Action.Complete;
    }

    @Override
    public void exception(Throwable cause) throws IOException {
      handler.handleError(cause, emptyList());
    }

  }

  @Override
  public ProtocolHandler createHandler() {
    return handler != null ? new SyncedHandler() : new LazyHandler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel
        .writeClose(objectType, objectName);

    if (handler != null) {
      channel
          .writeSync()
          .flush();
    }

  }

}
