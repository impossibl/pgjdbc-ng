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
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NegotiateProtocolVersion;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.utils.ByteBufs;

import static com.impossibl.postgres.utils.guava.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;

public class StartupRequest implements ServerRequest {

  interface CompletionHandler {

    String authenticateClear() throws IOException;
    String authenticateMD5(byte[] salt) throws IOException;
    void authenticateKerberos() throws IOException;
    byte authenticateSCM() throws IOException;
    ByteBuf authenticateGSS(ByteBuf data) throws IOException;
    ByteBuf authenticateGSSContinue(ByteBuf data) throws IOException;
    ByteBuf authenticateSSPI(ByteBuf data) throws IOException;
    ByteBuf authenticateSASL(List<String> mechanisms) throws IOException;
    ByteBuf authenticateSASLContinue(String serverFirstMessage) throws IOException;
    void authenticateSASLFinal(String serverFinalMessage) throws IOException;

    void handleNegotiate(Version maxProtocolVersion, List<String> unrecognizedParameters) throws IOException;

    void handleComplete(int processId, int secretKey, Map<String, String> parameterStatuses, List<Notice> notices) throws IOException;
    void handleError(Throwable cause, List<Notice> notices) throws IOException;

  }

  private Version protocolVersion;
  private Map<String, Object> startupParameters;
  private CompletionHandler handler;
  private int backendProcessId;
  private int backendSecretKey;
  private Map<String, String> parameterStatuses;
  private List<Notice> notices;

  StartupRequest(Version protocolVersion, Map<String, Object> startupParameters, CompletionHandler handler) {
    checkArgument(protocolVersion.getRevision() == null, "Protocol version cannot have a revision");
    this.protocolVersion = protocolVersion;
    this.startupParameters = startupParameters;
    this.handler = handler;
    this.parameterStatuses = new HashMap<>();
    this.notices = new ArrayList<>();
  }

  private class Handler implements Authentication, BackendKeyData, ParameterStatus, NegotiateProtocolVersion, ReadyForQuery, CommandError {

    @Override
    public String toString() {
      return "Startup";
    }

    @Override
    public Action negotiate(int maxSupportedMinorVersion, List<String> unrecognizedParameters) throws IOException {
      Version maxSupportedVersion = Version.get(protocolVersion.getMajor(), maxSupportedMinorVersion, null);
      handler.handleNegotiate(maxSupportedVersion, unrecognizedParameters);
      return Action.Resume;
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
      ByteBuf response = handler.authenticateGSS(data);
      try {
        channel
            .writePassword(response)
            .flush();
      }
      finally {
        response.release();
      }
    }

    @Override
    public void authenticateGSSContinue(ByteBuf data, ProtocolChannel channel) throws IOException {
      ByteBuf response = handler.authenticateGSSContinue(data);
      try {
        channel
            .writePassword(response)
            .flush();
      }
      finally {
        response.release();
      }
    }

    @Override
    public void authenticateSSPI(ByteBuf data, ProtocolChannel channel) throws IOException {
      ByteBuf response  = handler.authenticateSSPI(data);
      try {
        channel
            .writePassword(response)
            .flush();
      }
      finally {
        response.release();
      }
    }

    @Override
    public void authenticateSASL(ByteBuf data, ProtocolChannel channel) throws IOException {

      List<String> mechanisms = new ArrayList<>();
      while (true) {
        String mechanism = ByteBufs.readCString(data, UTF_8);
        if (mechanism.isEmpty()) {
          break;
        }
        mechanisms.add(mechanism);
      }

      ByteBuf response = handler.authenticateSASL(mechanisms);
      try {
        channel
            .writePassword(response)
            .flush();
      }
      finally {
        response.release();
      }
    }

    @Override
    public void authenticateSASLContinue(ByteBuf data, ProtocolChannel channel) throws IOException {

      String serverFirstMessage = data.readCharSequence(data.readableBytes(), UTF_8).toString();

      ByteBuf response = handler.authenticateSASLContinue(serverFirstMessage);
      try {
        channel
            .writePassword(response)
            .flush();
      }
      finally {
        response.release();
      }
    }

    @Override
    public void authenticateSASLFinal(ByteBuf data, ProtocolChannel channel) throws IOException {

      String serverFinalMessage = data.readCharSequence(data.readableBytes(), UTF_8).toString();

      handler.authenticateSASLFinal(serverFinalMessage);
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
        .writeStartup(protocolVersion.getMajor(), protocolVersion.getMinorValue(), startupParameters)
        .flush();

  }

}
