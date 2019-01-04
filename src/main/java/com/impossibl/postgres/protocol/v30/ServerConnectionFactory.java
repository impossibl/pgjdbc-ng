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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ssl.SSLEngineFactory;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Notification;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.system.Configuration;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.utils.MD5Authentication;
import com.impossibl.postgres.utils.StringTransforms;

import static com.impossibl.postgres.protocol.ServerConnection.KeyData;
import static com.impossibl.postgres.system.Settings.ALLOCATOR;
import static com.impossibl.postgres.system.Settings.ALLOCATOR_DEFAULT;
import static com.impossibl.postgres.system.Settings.APPLICATION_NAME;
import static com.impossibl.postgres.system.Settings.APPLICATION_NAME_DEFAULT;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING_DEFAULT;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE;
import static com.impossibl.postgres.system.Settings.MAX_MESSAGE_SIZE;
import static com.impossibl.postgres.system.Settings.MAX_MESSAGE_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.PROTOCOL_SOCKET_IO;
import static com.impossibl.postgres.system.Settings.PROTOCOL_SOCKET_IO_DEFAULT;
import static com.impossibl.postgres.system.Settings.PROTOCOL_SOCKET_IO_THREADS;
import static com.impossibl.postgres.system.Settings.PROTOCOL_SOCKET_IO_THREADS_DEFAULT;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE_DEFAULT;
import static com.impossibl.postgres.system.Settings.PROTOCOL_VERSION;
import static com.impossibl.postgres.system.Settings.PROTOCOL_VERSION_DEFAULT;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SSL_MODE;
import static com.impossibl.postgres.system.Settings.SSL_MODE_DEFAULT;
import static com.impossibl.postgres.utils.Await.awaitUninterruptibly;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;


public class ServerConnectionFactory implements com.impossibl.postgres.protocol.ServerConnectionFactory {

  private static final long DEFAULT_STARTUP_TIMEOUT = 60;
  private static final long DEFAULT_SSL_TIMEOUT = 60;


  public ServerConnection connect(Configuration config, SocketAddress address, ServerConnection.Listener listener) throws IOException {

    SSLMode sslMode = config.getSetting(SSL_MODE, SSL_MODE_DEFAULT, StringTransforms::capitalizeOption);

    return connect(config, sslMode, address, listener);
  }

  private ServerConnection connect(Configuration config, SSLMode sslMode, SocketAddress address, ServerConnection.Listener listener) throws IOException {

    try {

      ServerConnectionShared.Ref sharedRef = ServerConnectionShared.acquire();

      int maxMessageSize = config.getSetting(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE_DEFAULT);
      boolean usePooledAllocator = config.getSetting(ALLOCATOR, ALLOCATOR_DEFAULT);
      Charset clientEncoding = Charset.forName(config.getSetting(CLIENT_ENCODING, CLIENT_ENCODING_DEFAULT));

      Channel channel =
          createChannel(address, config, sharedRef, clientEncoding, maxMessageSize, usePooledAllocator)
              .syncUninterruptibly()
              .channel();

      if (sslMode != SSLMode.Disable && sslMode != SSLMode.Allow) {

        // Execute SSL query command

        SSLQueryRequest sslQueryRequest = new SSLQueryRequest();
        channel.writeAndFlush(sslQueryRequest).syncUninterruptibly();

        boolean sslQueryCompleted = awaitUninterruptibly(DEFAULT_SSL_TIMEOUT, SECONDS, sslQueryRequest::await);

        if (sslQueryCompleted && sslQueryRequest.isAllowed()) {

          // Attach the actual handler

          SSLEngine sslEngine = SSLEngineFactory.create(sslMode, config);

          final SslHandler sslHandler = new SslHandler(sslEngine);

          channel.pipeline().addFirst("ssl", sslHandler);

          try {

            sslHandler.handshakeFuture().syncUninterruptibly();

          }
          catch (Exception e) {

            // Retry with no SSL
            if (sslMode == SSLMode.Prefer) {
              return connect(config, SSLMode.Disable, address, listener);
            }

            throw e;
          }

        }
        else if (sslMode.isRequired()) {

          throw new IOException("SSL not allowed by server");
        }

      }

      try {

        Map<String, String> parameterStatuses = new HashMap<>();
        ServerConnection serverConnection = startup(config, channel, parameterStatuses, sharedRef);

        if (sslMode == SSLMode.VerifyFull) {

          SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
          if (sslHandler != null) {

            String hostname;
            if (address instanceof InetSocketAddress) {
              hostname = ((InetSocketAddress) address).getHostString();
            }
            else {
              hostname = "";
            }

            verifyHostname(hostname, sslHandler.engine().getSession());
          }

        }


        // Finally successfully connected...

        serverConnection.getMessageDispatchHandler().setDefaultHandler(new DefaultHandler(listener));

        parameterStatuses.forEach(listener::parameterStatusChanged);

        return serverConnection;
      }
      catch (Exception e) {

        switch (sslMode) {
          case Allow:
            return connect(config, SSLMode.Require, address, listener);

          case Prefer:
            return connect(config, SSLMode.Disable, address, listener);

          default:
            throw e;
        }

      }

    }
    catch (NoticeException e) {

      throw e;
    }
    catch (Exception e) {

      throw translateConnectionException(e);
    }

  }

  private ChannelFuture createChannel(SocketAddress address, Configuration config, ServerConnectionShared.Ref sharedRef,
                                      Charset clientEncoding, int maxMessageSize, boolean usePooledAllocator) {

    Bootstrap bootstrap;
    if (address instanceof InetSocketAddress) {
      bootstrap = bootstrapSocket(config, sharedRef, clientEncoding, maxMessageSize);
    }
    else {
      throw new IllegalArgumentException("Unsupported socket address: " + address.getClass().getSimpleName());
    }

    bootstrap
        .option(ChannelOption.ALLOCATOR, usePooledAllocator ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT);

    return bootstrap.connect(address);
  }

  @SuppressWarnings("deprecation")
  private Bootstrap bootstrapSocket(Configuration config, ServerConnectionShared.Ref sharedRef, Charset clientEncoding, int maxMessageSize) {

    Class<? extends SocketChannel> channelType;
    Class<? extends EventLoopGroup> groupType;
    int maxThreads;

    String ioMode = config.getSetting(PROTOCOL_SOCKET_IO, PROTOCOL_SOCKET_IO_DEFAULT).toLowerCase();
    switch (ioMode) {
      case "oio":
        channelType = io.netty.channel.socket.oio.OioSocketChannel.class;
        groupType = io.netty.channel.oio.OioEventLoopGroup.class;
        maxThreads = 0;
        break;

      case "nio":
        channelType = NioSocketChannel.class;
        groupType = NioEventLoopGroup.class;
        maxThreads = config.getSetting(PROTOCOL_SOCKET_IO_THREADS, PROTOCOL_SOCKET_IO_THREADS_DEFAULT);
        break;

      default:
        throw new IllegalStateException("Unsupported io mode: " + ioMode);
    }


    Writer protocolTraceWriter;
    if (config.getSetting(PROTOCOL_TRACE, PROTOCOL_TRACE_DEFAULT)) {
      protocolTraceWriter = new BufferedWriter(new OutputStreamWriter(System.out));
    }
    else {
      protocolTraceWriter = null;
    }

    Bootstrap bootstrap = new Bootstrap()
            .group(sharedRef.get().getEventLoopGroup(groupType, maxThreads))
            .channel(channelType)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(
                    new LengthFieldBasedFrameDecoder(maxMessageSize, 1, 4, -4, 0),
                    new MessageDispatchHandler(clientEncoding, protocolTraceWriter)
                );
              }
            })
            .option(ChannelOption.TCP_NODELAY, true);

    if (config.getSetting(RECEIVE_BUFFER_SIZE, RECEIVE_BUFFER_SIZE_DEFAULT) != RECEIVE_BUFFER_SIZE_DEFAULT) {
      bootstrap.option(ChannelOption.SO_RCVBUF, config.getSetting(RECEIVE_BUFFER_SIZE, int.class));
    }
    if (config.getSetting(SEND_BUFFER_SIZE, SEND_BUFFER_SIZE_DEFAULT) != SEND_BUFFER_SIZE_DEFAULT) {
      bootstrap.option(ChannelOption.SO_SNDBUF, config.getSetting(SEND_BUFFER_SIZE, int.class));
    }

    return bootstrap;
  }

  private static ServerConnection startup(Configuration config, Channel channel, Map<String, String> startupParameterStatuses, ServerConnectionShared.Ref sharedRef) throws IOException {

    Map<String, Object> params = new HashMap<>();
    params.put(APPLICATION_NAME, config.getSetting(APPLICATION_NAME, APPLICATION_NAME_DEFAULT));
    params.put(CLIENT_ENCODING, config.getSetting(CLIENT_ENCODING, CLIENT_ENCODING_DEFAULT));
    params.put(DATABASE, config.getSetting(DATABASE, ""));
    params.put(CREDENTIALS_USERNAME, config.getSetting(CREDENTIALS_USERNAME, ""));

    Version protocolVersion = Version.parse(config.getSetting(PROTOCOL_VERSION, PROTOCOL_VERSION_DEFAULT));

    AtomicReference<Version> startupProtocolVersion = new AtomicReference<>();
    AtomicReference<KeyData> startupKeyData = new AtomicReference<>();
    AtomicReference<Throwable> startupError = new AtomicReference<>();
    CountDownLatch startupCompleted = new CountDownLatch(1);

    StartupRequest startupRequest = new StartupRequest(protocolVersion, params, new StartupRequest.CompletionHandler() {
      @Override
      public String authenticateClear() {
        return config.getSetting(CREDENTIALS_PASSWORD, "");
      }

      @Override
      public String authenticateMD5(byte[] salt) {

        String username = config.getSetting(CREDENTIALS_USERNAME).toString();
        String password = config.getSetting(CREDENTIALS_PASSWORD).toString();

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
      public ByteBuf authenticateSSPI(ByteBuf data) {
        throw new IllegalStateException("Unsupported Authentication Method");
      }

      @Override
      public ByteBuf authenticateContinue(ByteBuf data) {
        throw new IllegalStateException("Unsupported Authentication Method");
      }

      @Override
      public void handleNegotiate(Version maxProtocolVersion, List<String> unrecognizedParameters) {
        startupProtocolVersion.set(maxProtocolVersion);
      }

      @Override
      public void handleComplete(Integer processId, Integer secretKey, Map<String, String> parameterStatuses, List<Notice> notices) {

        startupParameterStatuses.putAll(parameterStatuses);
        startupKeyData.set(new KeyData(processId, secretKey));

        startupCompleted.countDown();
      }

      @Override
      public void handleError(Throwable error, List<Notice> notices) {
        startupError.set(error);
        startupCompleted.countDown();
      }

    });
    channel.writeAndFlush(startupRequest).syncUninterruptibly();

    if (!awaitUninterruptibly(DEFAULT_STARTUP_TIMEOUT, SECONDS, startupCompleted::await)) {
      throw new IOException("Timeout starting connection");
    }

    if (startupError.get() != null) {
      Throwable error = startupError.get();
      if (error instanceof IOException) throw (IOException) error;
      if (error instanceof RuntimeException) throw (RuntimeException) error;
      throw new RuntimeException(error);
    }

    // Pull out static server parameters
    ServerInfo serverInfo = new ServerInfo(
        Version.parse(startupParameterStatuses.remove("server_version")),
        startupParameterStatuses.remove("server_encoding"),
        firstNonNull(startupParameterStatuses.remove("integer_datetimes"), "on").equalsIgnoreCase("on")
    );

    protocolVersion = startupProtocolVersion.get() != null ? startupProtocolVersion.get() : protocolVersion;

    return new ServerConnection(config, channel, serverInfo, protocolVersion, startupKeyData.get(), sharedRef);
  }

  private void verifyHostname(String hostname, SSLSession session) throws SSLPeerUnverifiedException {

    X509Certificate[] peerCerts = (X509Certificate[]) session.getPeerCertificates();
    if (peerCerts == null || peerCerts.length == 0) {
      throw new SSLPeerUnverifiedException("No peer certificates");
    }

    // Extract the common name
    X509Certificate serverCert = peerCerts[0];
    LdapName DN;
    try {
      DN = new LdapName(serverCert.getSubjectX500Principal().getName(X500Principal.RFC2253));
    }
    catch (InvalidNameException e) {
      throw new SSLPeerUnverifiedException("Invalid name in certificate");
    }

    String CN = null;
    for (Rdn rdn : DN.getRdns()) {
      if ("CN".equals(rdn.getType())) {
        // Multiple AVAs are not treated
        CN = (String) rdn.getValue();
        break;
      }
    }

    if (CN == null) {
      throw new SSLPeerUnverifiedException("Common name not found");
    }
    else if (CN.startsWith("*")) {

      // We have a wildcard
      if (hostname.endsWith(CN.substring(1))) {
        // Avoid IndexOutOfBoundsException because hostname already ends with CN
        if (!(hostname.substring(0, hostname.length() - CN.length() + 1).contains("."))) {
          throw new SSLPeerUnverifiedException("The hostname " + hostname + " could not be verified");
        }
      }
      else {
        throw new SSLPeerUnverifiedException("The hostname " + hostname + " could not be verified");
      }

    }
    else {
      if (!CN.equals(hostname)) {
        throw new SSLPeerUnverifiedException("The hostname " + hostname + " could not be verified");
      }
    }
  }

  private static IOException translateConnectionException(Exception e) {

    IOException io;

    // Unwrap
    if (e instanceof IOException) {
      io = (IOException) e;
    }
    else if (e.getCause() == null) {
      io = new IOException(e);
    }
    else if (e.getCause() instanceof IOException) {
      io = (IOException) e.getCause();
    }
    else {
      io = new IOException(e.getCause());
    }

    // Unwrap SSL Handshake exceptions

    while (io instanceof SSLHandshakeException) {
      if (io.getCause() instanceof IOException) {
        io = (IOException) io.getCause();
      }
      else if (io.getCause() != null) {
        io = new SSLException(io.getCause().getMessage(), io.getCause());
      }
      else {
        io = new SSLException(io.getMessage(), io);
      }
    }

    if (io instanceof SSLException) {
      if (!io.getMessage().startsWith("SSL Error"))
        io = new SSLException("SSL Error: " + io.getMessage(), io.getCause());
    }

    return io;
  }

  static class DefaultHandler implements ParameterStatus, ReportNotice, Notification, CommandError {

    private static final Logger logger = Logger.getLogger(ServerConnection.class.getName());

    private WeakReference<ServerConnection.Listener> listener;

    DefaultHandler(ServerConnection.Listener listener) {
      this.listener = new WeakReference<>(listener);
    }

    private ServerConnection.Listener getListener() {
      return listener.get();
    }

    @Override
    public String toString() {
      return "DEFAULT";
    }

    @Override
    public Action parameterStatus(String name, String value) {
      ServerConnection.Listener listener = getListener();
      if (listener != null) {
        listener.parameterStatusChanged(name, value);
      }
      return Action.Resume;
    }

    @Override
    public void notification(int processId, String channelName, String payload) {
      ServerConnection.Listener listener = getListener();
      if (listener != null) {
        listener.notificationReceived(processId, channelName, payload);
      }
    }

    @Override
    public void exception(Channel channel, Throwable cause) {
      if (!channel.isOpen()) {
        ServerConnection.Listener listener = getListener();
        if (listener != null) {
          listener.closed();
        }
      }
    }

    @Override
    public void exception(Throwable cause) {
      if (cause instanceof ClosedChannelException) return;
      logger.log(Level.WARNING, "Unhandled connection exception", cause);
    }

    @Override
    public Action notice(Notice notice) {
      return null;
    }

    @Override
    public Action error(Notice notice) {
      logger.warning(notice.getMessage());
      return Action.Resume;
    }
  }

}
