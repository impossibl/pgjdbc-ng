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
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Authentication;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BackendKeyData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.utils.MD5Authentication;

import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StartupRequest implements ServerRequest {

  interface CompletionHandler {

    void handleComplete(Integer processId, Integer secretKey, Map<String, String> parameterStatuses, List<Notice> notices) throws IOException;
    void handleError(Throwable cause, List<Notice> notices) throws IOException;

  }

  private Map<String, Object> startupParameters;
  private CompletionHandler handler;
  private Integer backendProcessId;
  private Integer backendSecretKey;
  private Map<String, String> parameterStatuses;
  private List<Notice> notices;

  StartupRequest(Map<String, Object> startupParameters, CompletionHandler handler) {
    this.startupParameters = startupParameters;
    this.handler = handler;
    this.parameterStatuses = new HashMap<>();
    this.notices = new ArrayList<>();
  }

  private class Handler implements Authentication, BackendKeyData, ParameterStatus, ReadyForQuery, CommandError {

    @Override
    public Action backendKeyData(int processId, int secretKey) {
      backendProcessId = processId;
      backendSecretKey = secretKey;
      return Action.Resume;
    }

    @Override
    public Action parameterStatus(String name, String value) {
      parameterStatuses.put(name, value);
      return Action.Resume;
    }

    @Override
    public Action authenticated() {
      return Action.Resume;
    }

    @Override
    public void authenticateClear(Context context, ProtocolChannel channel) {

      channel
          .writePassword(context.getSetting(CREDENTIALS_PASSWORD, ""))
          .flush();

    }

    @Override
    public void authenticateMD5(Context context, byte[] salt, ProtocolChannel channel) {

      String username = context.getSetting(CREDENTIALS_USERNAME).toString();
      String password = context.getSetting(CREDENTIALS_PASSWORD).toString();

      String response = MD5Authentication.encode(password, username, salt);

      channel
          .writePassword(response)
          .flush();
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) throws IOException {
      handler.handleComplete(backendProcessId, backendSecretKey, parameterStatuses, notices);
      return Action.Complete;
    }

    @Override
    public Action error(Notice error) throws IOException {
      handler.handleError(new NoticeException(error), notices);
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

    channel
        .writeStartup(startupParameters)
        .flush();

  }

}
