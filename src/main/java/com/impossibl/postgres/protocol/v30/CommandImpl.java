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

import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.v30.ProtocolImpl.ExecutionTimerTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public abstract class CommandImpl implements Command {

  protected long networkTimeout;
  protected Throwable exception;
  protected Notice error;
  protected List<Notice> notices;

  @Override
  public void setNetworkTimeout(long timeout) {
    this.networkTimeout = timeout;
  }

  @Override
  public Notice getError() {
    return error;
  }

  public void setError(Notice error) {
    this.error = error;
  }

  @Override
  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable cause) {
    this.exception = cause;
  }

  void addNotice(Notice notice) {

    if (notices == null)
      notices = new ArrayList<>();

    notices.add(notice);
  }

  @Override
  public List<Notice> getWarnings() {

    if (notices == null)
      return emptyList();

    List<Notice> warnings = new ArrayList<>();

    for (Notice notice : notices) {

      if (notice.isWarning())
        warnings.add(notice);
    }

    return warnings;
  }

  static class CancelExecutionTimerTask extends ExecutionTimerTask {

    ProtocolImpl protocol;

    CancelExecutionTimerTask(ProtocolImpl protocol) {
      this.protocol = protocol;
    }

    @Override
    public void run() {

      protocol.sendCancelRequest();

    }

  }

  void enableCancelTimer(final ProtocolImpl protocol, long timeout) {

    if (timeout < 1)
      return;

    protocol.enableExecutionTimer(new CancelExecutionTimerTask(protocol), timeout);

  }

  public abstract void execute(ProtocolImpl protocol) throws IOException;

}
