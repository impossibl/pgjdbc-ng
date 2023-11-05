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

import com.impossibl.postgres.protocol.sasl.scram.client.ScramSession;
import com.impossibl.postgres.protocol.sasl.scram.client.ScramSessionFactory;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramException;
import com.impossibl.postgres.protocol.sasl.scram.stringprep.StringPreparations;
import com.impossibl.postgres.system.Configuration;
import com.impossibl.postgres.utils.ByteBufs;
import com.impossibl.postgres.utils.MD5Authentication;

import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.SystemSettings.SSL_MODE;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;

abstract class AuthenticationHandler implements StartupRequest.CompletionHandler {

  private static final String SCRAM_CHANNEL_BIND_METHOD = "tls-server-end-point";

  private final Configuration config;
  private final Channel channel;
  private ScramSession scramSession;

  AuthenticationHandler(Configuration config, Channel channel) {
    this.config = config;
    this.channel = channel;
  }

  @Override
  public String authenticateClear() {
    return config.getSetting(CREDENTIALS_PASSWORD);
  }

  @Override
  public String authenticateMD5(byte[] salt) {

    String username = config.getSetting(CREDENTIALS_USERNAME);
    String password = config.getSetting(CREDENTIALS_PASSWORD);

    return MD5Authentication.encode(password, username, salt);
  }

  @Override
  public void authenticateKerberos() {
    throw new IllegalStateException("Unsupported Authentication Method");
  }

  @Override
  public byte authenticateSCM() {
    throw new IllegalStateException("Unsupported Authentication Method");
  }

  @Override
  public ByteBuf authenticateGSS(ByteBuf data) {
    throw new IllegalStateException("Unsupported Authentication Method");
  }

  @Override
  public ByteBuf authenticateGSSContinue(ByteBuf data) {
    throw new IllegalStateException("Unsupported Authentication Method");
  }

  @Override
  public ByteBuf authenticateSSPI(ByteBuf data) {
    throw new IllegalStateException("Unsupported Authentication Method");
  }

  @Override
  public ByteBuf authenticateSASL(List<String> mechanisms) throws IOException {
    SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
    boolean clientSupportsChannelBinding = sslHandler != null &&
        sslHandler.engine().getSession().getPeerCertificates() != null &&
        sslHandler.engine().getSession().getPeerCertificates().length > 0;

    ScramSessionFactory scramSessionFactory;
    try {
      scramSessionFactory =
          ScramSessionFactory.builder()
              .serverAdvertisedMechanisms(mechanisms)
              .channelBindMethod(clientSupportsChannelBinding ? SCRAM_CHANNEL_BIND_METHOD : null)
              .stringPreparation(StringPreparations.SASL_PREPARATION)
              .preferChannelBindingMechanism(config.getSetting(SSL_MODE).isRequired())
              .build();
    }
    catch (IllegalArgumentException e) {
      throw new IOException("No supported SASL mechanisms available");
    }

    scramSession = scramSessionFactory.start("");

    ByteBuf response = channel.alloc().buffer();

    ByteBufs.writeCString(response, scramSession.getScramMechanismName(), UTF_8);

    byte[] clientFirstMessageData = scramSession.clientFirstMessage(null);
    response.writeInt(clientFirstMessageData.length);
    response.writeBytes(clientFirstMessageData);

    return response;
  }

  @Override
  public ByteBuf authenticateSASLContinue(String serverFirstMessage) throws IOException {

    byte[] channelBindData;
    if (scramSession.requiresChannelBindData()) {
      if (!SCRAM_CHANNEL_BIND_METHOD.equals(scramSession.getChannelBindMethod())) {
        throw new ScramException("Unsupported channel-bind method");
      }

      try {
        // Retrieve certificate for generating 'tls-server-end-point' channel binding data
        SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
        X509Certificate[] peerCerts = (X509Certificate[]) sslHandler.engine().getSession().getPeerCertificates();
        X509Certificate peerCert = peerCerts[0];

        // Generate channel binding data via SHA-256 of certificate
        channelBindData = MessageDigest.getInstance("SHA-256").digest(peerCert.getEncoded());
      }
      catch (Exception e) {
        throw new ScramException("Failed to generate channel-bind data", e);
      }
    }
    else {
      channelBindData = null;
    }

    String password = config.getSetting(CREDENTIALS_PASSWORD);

    byte[] clientFinalMessage = scramSession.receiveServerFirstMessage(serverFirstMessage, channelBindData, password);

    ByteBuf response = channel.alloc().buffer(clientFinalMessage.length);
    response.writeBytes(clientFinalMessage);
    return response;
  }

  @Override
  public void authenticateSASLFinal(String serverFinalMessage) throws IOException {
    scramSession.receiveServerFinalMessage(serverFinalMessage);
  }

}
