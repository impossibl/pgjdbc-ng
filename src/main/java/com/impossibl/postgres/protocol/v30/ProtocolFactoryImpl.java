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
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.ProtocolFactory;
import com.impossibl.postgres.protocol.SSLRequestCommand;
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.protocol.ssl.SSLContextFactory;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.utils.Converter;

import static com.impossibl.postgres.system.Settings.APPLICATION_NAME;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE;
import static com.impossibl.postgres.system.Settings.SSL_MODE;
import static com.impossibl.postgres.system.Settings.SSL_MODE_DEFAULT;
import static com.impossibl.postgres.utils.StringTransforms.capitalizeOption;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.ssl.SslHandler;

public class ProtocolFactoryImpl implements ProtocolFactory {

  @Override
  public Protocol connect(SocketAddress address, BasicContext context) throws IOException, NoticeException {

    SSLMode sslMode = context.getSetting(SSL_MODE, new Converter<SSLMode>() {

      @Override
      public SSLMode apply(Object val) {
        if (val == null)
          return SSL_MODE_DEFAULT;
        String valStr = capitalizeOption(val.toString());
        return SSLMode.valueOf(valStr);
      }

    });

    return connect(sslMode, address, context);
  }

  Protocol connect(SSLMode sslMode, SocketAddress address, BasicContext context) throws IOException, NoticeException {

    try {

      ProtocolShared.Ref sharedRef = ProtocolShared.acquire();

      ClientBootstrap clientBootstrap = sharedRef.get().getBootstrap();

      ChannelFuture connectFuture = clientBootstrap.connect(address).syncUninterruptibly();

      Channel channel = connectFuture.getChannel();
      ProtocolImpl protocol = new ProtocolImpl(sharedRef, channel, context);

      channel.setAttachment(protocol);

      if (sslMode != SSLMode.Disable && sslMode != SSLMode.Allow) {

        // Execute SSL request command

        SSLRequestCommand sslRequestCommand = protocol.createSSLRequest();
        if (sslRequestCommand == null && sslMode.isRequired()) {

          throw new IOException("SSL not available");
        }

        protocol.execute(sslRequestCommand);

        // Did server allow it?

        if (sslRequestCommand.isAllowed()) {

          // Attach the actual handler

          SSLEngine sslEngine = SSLContextFactory.create(sslMode, context).createSSLEngine();

          sslEngine.setUseClientMode(true);

          SslHandler sslHandler = new SslHandler(sslEngine);

          channel.getPipeline().addFirst("ssl", sslHandler);

          try {

            sslHandler.handshake().syncUninterruptibly();

          }
          catch (Exception e) {

            // Retry with no SSL
            if (sslMode == SSLMode.Prefer) {
              return connect(SSLMode.Disable, address, context);
            }

            throw e;
          }

        }
        else if (sslMode.isRequired()) {

          throw new IOException("SSL not allowed by server");
        }

      }

      try {

        startup(protocol, context);

      }
      catch (Exception e) {

        switch (sslMode) {
          case Allow:
            return connect(SSLMode.Require, address, context);

          case Prefer:
            return connect(SSLMode.Disable, address, context);

          default:
            throw e;
        }

      }

      return protocol;
    }
    catch (ChannelException e) {

      IOException io;

      // Unwrap
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        io = (IOException) cause;
      }
      else {
        io = new IOException(cause);
      }

      // Unwrap SSL Handshake exceptions

      while (io instanceof SSLHandshakeException) {
        if (io.getCause() instanceof IOException) {
          io = (IOException) io.getCause();
        }
        else {
          io = new IOException("SSL Error: " + io.getCause().getMessage(), io.getCause());
        }
      }

      throw io;
    }

  }

  private void startup(ProtocolImpl protocol, BasicContext context) throws IOException, NoticeException {

    Map<String, Object> params = new HashMap<String, Object>();

    params.put(APPLICATION_NAME, "pgjdbc app");
    params.put(CLIENT_ENCODING, "UTF8");
    params.put(DATABASE, context.getSetting(DATABASE, ""));
    params.put(CREDENTIALS_USERNAME, context.getSetting(CREDENTIALS_USERNAME, ""));

    StartupCommand startup = protocol.createStartup(params);

    protocol.execute(startup);

    Notice error = startup.getError();
    if (error != null) {
      throw new NoticeException("Startup Failed", error);
    }

  }

}
