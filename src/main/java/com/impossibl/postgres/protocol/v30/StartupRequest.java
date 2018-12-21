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
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class StartupRequest implements ServerRequest {

  interface CompletionHandler {

    String authenticateClear() throws IOException;
    String authenticateMD5(byte[] salt) throws IOException;
    void authenticateKerberos() throws IOException;
    byte authenticateSCM() throws IOException;
    ByteBuf authenticateGSS(ByteBuf data) throws IOException;
    ByteBuf authenticateSSPI(ByteBuf data) throws IOException;
    ByteBuf authenticateContinue(ByteBuf data) throws IOException;

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
    public String toString() {
      return "Startup";
    }

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
    public void authenticateClear(ProtocolChannel channel) throws IOException {

      String password = handler.authenticateClear();

      channel
          .writePassword(password)
          .flush();

    }

    @Override
    public void authenticateMD5(byte[] salt, ProtocolChannel channel) throws IOException {

      String response = handler.authenticateMD5(salt);

      channel
          .writePassword(response)
          .flush();
    }

    @Override
    public void authenticateKerberos(ProtocolChannel channel) throws IOException {
      handler.authenticateKerberos();
    }

    @Override
    public void authenticateSCM(ProtocolChannel channel) throws IOException {

      byte response = handler.authenticateSCM();

      channel
          .writeSCM(response)
          .flush();

    }

    @Override
    public void authenticateGSS(ByteBuf data, ProtocolChannel channel) throws IOException {

      data = handler.authenticateGSS(data);

      channel
          .writePassword(data)
          .flush();

      data.release();
    }

    @Override
    public void authenticateSSPI(ByteBuf data, ProtocolChannel channel) throws IOException {

      data = handler.authenticateSSPI(data);

      channel
          .writePassword(data)
          .flush();

      data.release();
    }

    @Override
    public void authenticateContinue(ByteBuf data, ProtocolChannel channel) throws IOException {

      data = handler.authenticateContinue(data);

      channel
          .writePassword(data)
          .flush();

      data.release();
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
