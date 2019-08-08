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

import com.impossibl.postgres.protocol.CopyFormat;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ssl.SSLEngineFactory;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyDone;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyFail;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyInResponse;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CopyOutResponse;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Notification;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.system.Configuration;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.ParameterNames;
import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.system.SystemSettings;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.utils.MD5Authentication;

import static com.impossibl.postgres.protocol.ServerConnection.KeyData;
import static com.impossibl.postgres.system.SystemSettings.APPLICATION_NAME;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_NAME;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_BUFFER_POOLING;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_ENCODING;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_IO_MODE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_IO_THREADS;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_MESSAGE_SIZE_MAX;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_RECV_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_TRACE_FILE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_VERSION;
import static com.impossibl.postgres.system.SystemSettings.SSL_MODE;
import static com.impossibl.postgres.utils.Await.awaitUninterruptibly;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;


public class ServerConnectionFactory implements com.impossibl.postgres.protocol.ServerConnectionFactory {

  private static final long DEFAULT_STARTUP_TIMEOUT = 60;
  private static final long DEFAULT_SSL_TIMEOUT = 60;

  static class CreatedChannel {
    ServerConnectionShared.Ref sharedRef;
    ChannelFuture channelFuture;

    CreatedChannel(ServerConnectionShared.Ref sharedRef, ChannelFuture channelFuture) {
      this.sharedRef = sharedRef;
      this.channelFuture = channelFuture;
    }
  }

  public ServerConnection connect(Configuration config, SocketAddress address, ServerConnection.Listener listener) throws IOException {

    SSLMode sslMode = config.getSetting(SSL_MODE);

    return connect(config, sslMode, address, listener, 1);
  }

  private ServerConnection connect(Configuration config, SSLMode sslMode, SocketAddress address, ServerConnection.Listener listener, int attempt) throws IOException {

    try {

      CreatedChannel createdChannel = createChannel(address, config);

      ServerConnectionShared.Ref sharedRef = createdChannel.sharedRef;
      Channel channel = createdChannel.channelFuture.syncUninterruptibly().channel();

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
              return connect(config, SSLMode.Disable, address, listener, attempt);
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
            return connect(config, SSLMode.Require, address, listener, attempt);

          case Prefer:
            return connect(config, SSLMode.Disable, address, listener, attempt);

          default:
            // WORKAROUND: ISSUE#392: Retrying random startup disconnect mitigates failures.
            if (e instanceof ClosedChannelException && attempt < 2) {
              return connect(config, sslMode, address, listener, attempt + 1);
            }
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

  private CreatedChannel createChannel(SocketAddress address, Configuration config) {

    if (address instanceof InetSocketAddress) {
      return createInetSocketChannel((InetSocketAddress) address, config);
    }
    else if (address instanceof DomainSocketAddress) {
      return createDomainSocketChannel((DomainSocketAddress) address, config);
    }
    else {
      throw new IllegalArgumentException("Unsupported socket address: " + address.getClass().getSimpleName());
    }
  }

  @SuppressWarnings("deprecation")
  private CreatedChannel createInetSocketChannel(InetSocketAddress address, Configuration config) {

    int maxMessageSize = config.getSetting(PROTOCOL_MESSAGE_SIZE_MAX);
    Charset clientEncoding = config.getSetting(PROTOCOL_ENCODING);

    Class<? extends SocketChannel> channelType;
    Class<? extends EventLoopGroup> groupType;

    int maxThreads = config.getSetting(PROTOCOL_IO_THREADS);

    SystemSettings.ProtocolIOMode ioMode = config.getSetting(PROTOCOL_IO_MODE);
    switch (ioMode) {
      case OIO:
        channelType = io.netty.channel.socket.oio.OioSocketChannel.class;
        groupType = io.netty.channel.oio.OioEventLoopGroup.class;
        maxThreads = 0;
        break;

      case ANY:

        // Fallthrough to try in order...

      case NATIVE:
        if (KQueue.isAvailable()) {
          channelType = KQueueSocketChannel.class;
          groupType = KQueueEventLoopGroup.class;
          break;
        }
        else if (Epoll.isAvailable()) {
          channelType = EpollSocketChannel.class;
          groupType = EpollEventLoopGroup.class;
          break;
        }
        else if (ioMode != SystemSettings.ProtocolIOMode.ANY) {
          throw new IllegalStateException("Unsupported io mode: native: no native library loaded");
        }

      case NIO:
        channelType = NioSocketChannel.class;
        groupType = NioEventLoopGroup.class;
        break;

      default:
        throw new IllegalStateException("Unsupported io mode: " + ioMode);
    }

    ServerConnectionShared.Ref sharedRef = ServerConnectionShared.acquire(groupType, maxThreads);

    Writer protocolTraceWriter = createProtocolTracer(config);

    Bootstrap bootstrap = new Bootstrap()
            .group(sharedRef.get().getEventLoopGroup())
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

    configureChannelOptions(config, bootstrap);

    ChannelFuture channelFuture = bootstrap.connect(address);

    return new CreatedChannel(sharedRef, channelFuture);
  }

  private CreatedChannel createDomainSocketChannel(DomainSocketAddress address, Configuration config) {

    int maxMessageSize = config.getSetting(PROTOCOL_MESSAGE_SIZE_MAX);
    Charset clientEncoding = config.getSetting(PROTOCOL_ENCODING);

    Class<? extends DomainSocketChannel> channelType;
    Class<? extends EventLoopGroup> groupType;
    if (KQueue.isAvailable()) {
      channelType = KQueueDomainSocketChannel.class;
      groupType = KQueueEventLoopGroup.class;
    }
    else if (Epoll.isAvailable()) {
      channelType = EpollDomainSocketChannel.class;
      groupType = EpollEventLoopGroup.class;
    }
    else {
      throw new IllegalArgumentException("Unix domain sockets not supported: missing native libraries");
    }

    int maxThreads = config.getSetting(PROTOCOL_IO_THREADS);

    ServerConnectionShared.Ref sharedRef = ServerConnectionShared.acquire(groupType, maxThreads);

    Writer protocolTraceWriter = createProtocolTracer(config);

    Bootstrap bootstrap = new Bootstrap()
        .group(sharedRef.get().getEventLoopGroup())
        .channel(channelType)
        .handler(new ChannelInitializer<DomainSocketChannel>() {
          @Override
          protected void initChannel(DomainSocketChannel ch) {
            ch.pipeline().addLast(
                new LengthFieldBasedFrameDecoder(maxMessageSize, 1, 4, -4, 0),
                new MessageDispatchHandler(clientEncoding, protocolTraceWriter)
            );
          }
        });

    configureChannelOptions(config, bootstrap);

    ChannelFuture channelFuture = bootstrap.connect(address);

    return new CreatedChannel(sharedRef, channelFuture);
  }

  private void configureChannelOptions(Configuration config, Bootstrap bootstrap) {

    Integer receiveBufferSize = config.getSetting(PROTOCOL_SOCKET_RECV_BUFFER_SIZE);
    if (receiveBufferSize != null) {
      bootstrap.option(ChannelOption.SO_RCVBUF, receiveBufferSize);
    }

    Integer sendBufferSize = config.getSetting(PROTOCOL_SOCKET_SEND_BUFFER_SIZE);
    if (sendBufferSize != null) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sendBufferSize);
    }

    boolean usePooledAllocator = config.getSetting(PROTOCOL_BUFFER_POOLING);
    bootstrap.option(ChannelOption.ALLOCATOR, usePooledAllocator ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT);
  }

  private Writer createProtocolTracer(Configuration config) {
    if (config.getSetting(PROTOCOL_TRACE)) {
      OutputStream out = System.out;
      String filePath = config.getSetting(PROTOCOL_TRACE_FILE);
      if (filePath != null) {
        try {
          out = new FileOutputStream(filePath, false);
        }
        catch (FileNotFoundException ignored) {
        }
      }
      return new BufferedWriter(new OutputStreamWriter(out));
    }
    return null;
  }

  private static ServerConnection startup(Configuration config, Channel channel, Map<String, String> startupParameterStatuses, ServerConnectionShared.Ref sharedRef) throws IOException {

    Map<String, Object> params = new HashMap<>();
    params.put(ParameterNames.APPLICATION_NAME, config.getSetting(APPLICATION_NAME));
    params.put(ParameterNames.CLIENT_ENCODING, config.getSetting(PROTOCOL_ENCODING));
    params.put(ParameterNames.DATABASE, config.getSetting(DATABASE_NAME));
    params.put(ParameterNames.USER, config.getSetting(CREDENTIALS_USERNAME));

    Version protocolVersion = config.getSetting(PROTOCOL_VERSION);

    AtomicReference<Version> startupProtocolVersion = new AtomicReference<>();
    AtomicReference<KeyData> startupKeyData = new AtomicReference<>();
    AtomicReference<Throwable> startupError = new AtomicReference<>();
    CountDownLatch startupCompleted = new CountDownLatch(1);

    StartupRequest startupRequest = new StartupRequest(protocolVersion, params, new StartupRequest.CompletionHandler() {
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
      public void handleComplete(int processId, int secretKey, Map<String, String> parameterStatuses, List<Notice> notices) {

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
    if (e instanceof ClosedChannelException) {
      io = new IOException("Channel Closed", e);
    }
    else if (e instanceof IOException) {
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

  static class DefaultHandler implements ParameterStatus, ReportNotice, Notification, CopyInResponse, CopyOutResponse, CommandError {

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
    public InputStream copyIn(CopyFormat format, FieldFormat[] fieldFormats) {
      ServerConnection.Listener listener = getListener();
      if (listener == null) return null;
      return listener.openStandardInput();
    }

    @Override
    public ProtocolHandler copyOut(CopyFormat format, FieldFormat[] fieldFormats) {
      ServerConnection.Listener listener = getListener();
      if (listener == null) return null;
      return new DefaultCopyOutHandler(listener.openStandardOutput());
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

  static class DefaultCopyOutHandler implements CopyData, CopyDone, CopyFail {

    OutputStream stream;

    DefaultCopyOutHandler(OutputStream stream) {
      this.stream = stream;
    }

    @Override
    public void copyData(ByteBuf data) throws IOException {

      while (data.isReadable()) {
        data.readBytes(stream, data.readableBytes());
      }

    }

    @Override
    public void copyDone() {
    }

    public void copyFail(String message) {
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

}
