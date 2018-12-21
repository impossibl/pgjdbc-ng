package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ssl.SSLEngineFactory;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Notification;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.utils.MD5Authentication;

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
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SSL_MODE;
import static com.impossibl.postgres.system.Settings.SSL_MODE_DEFAULT;
import static com.impossibl.postgres.utils.Await.awaitUninterruptibly;
import static com.impossibl.postgres.utils.StringTransforms.capitalizeOption;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

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
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;


public class ServerConnectionFactory implements com.impossibl.postgres.protocol.ServerConnectionFactory {

  private static final long DEFAULT_STARTUP_TIMEOUT = 60;
  private static final long DEFAULT_SSL_TIMEOUT = 60;


  public ServerConnection connect(SocketAddress address, BasicContext context) throws IOException, NoticeException {

    SSLMode sslMode = context.getSetting(SSL_MODE, val -> {
      if (val == null)
        return SSL_MODE_DEFAULT;
      String valStr = capitalizeOption(val.toString());
      return SSLMode.valueOf(valStr);
    });

    return connect(sslMode, address, context);
  }

  private ServerConnection connect(SSLMode sslMode, SocketAddress address, BasicContext context) throws IOException, NoticeException {

    try {

      ServerConnectionShared.Ref sharedRef = ServerConnectionShared.acquire();

      Charset clientEncoding = Charset.forName(context.getSetting(CLIENT_ENCODING, CLIENT_ENCODING_DEFAULT));
      int maxMessageSize = context.getSetting(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE_DEFAULT);
      boolean usePooledAllocator = context.getSetting(ALLOCATOR, ALLOCATOR_DEFAULT);

      Channel channel =
          createChannel(address, context, sharedRef, clientEncoding, maxMessageSize, usePooledAllocator)
              .syncUninterruptibly()
              .channel();

      ServerConnection serverConnection = new ServerConnection(context, channel, sharedRef);

      if (sslMode != SSLMode.Disable && sslMode != SSLMode.Allow) {

        // Execute SSL query command

        SSLQueryRequest sslQueryRequest = new SSLQueryRequest();
        serverConnection.submit(sslQueryRequest);

        boolean sslQueryCompleted = awaitUninterruptibly(DEFAULT_SSL_TIMEOUT, SECONDS, sslQueryRequest::await);

        if (sslQueryCompleted && sslQueryRequest.isAllowed()) {

          // Attach the actual handler

          SSLEngine sslEngine = SSLEngineFactory.create(sslMode, context);

          final SslHandler sslHandler = new SslHandler(sslEngine);

          channel.pipeline().addFirst("ssl", sslHandler);

          try {

            sslHandler.handshakeFuture().syncUninterruptibly();

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

        startup(serverConnection, context);

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

      return serverConnection;
    }
    catch (NoticeException e) {

      throw e;
    }
    catch (Exception e) {

      throw translateConnectionException(e);
    }

  }

  private ChannelFuture createChannel(SocketAddress address, BasicContext context, ServerConnectionShared.Ref sharedRef, Charset clientEncoding, int maxMessageSize, boolean usePooledAllocator) {

    Bootstrap bootstrap;
    if (address instanceof InetSocketAddress) {
      bootstrap = bootstrapSocket(context, sharedRef, clientEncoding, maxMessageSize);
    }
    else {
      throw new IllegalArgumentException("Unsupported socket address: " + address.getClass().getSimpleName());
    }

    bootstrap
        .option(ChannelOption.ALLOCATOR, usePooledAllocator ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT);

    return bootstrap.connect(address);
  }

  private Bootstrap bootstrapSocket(BasicContext context, ServerConnectionShared.Ref sharedRef, Charset clientEncoding, int maxMessageSize) {

    Class<? extends SocketChannel> channelType;
    Class<? extends EventLoopGroup> groupType;

    String ioMode = context.getSetting(PROTOCOL_SOCKET_IO, PROTOCOL_SOCKET_IO_DEFAULT).toLowerCase();
    switch (ioMode) {
      case "oio":
        channelType = OioSocketChannel.class;
        groupType = OioEventLoopGroup.class;
        break;

      case "nio":
        channelType = NioSocketChannel.class;
        groupType = NioEventLoopGroup.class;
        break;

      default:
        throw new IllegalStateException("Unsupported io mode: " + ioMode);
    }

    Bootstrap bootstrap = new Bootstrap()
            .group(sharedRef.get().getEventLoopGroup(groupType))
            .channel(channelType)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(
                    new LengthFieldBasedFrameDecoder(maxMessageSize, 1, 4, -4, 0),
                    new MessageDispatchHandler(clientEncoding, new DefaultHandler(context))
                );
              }
            })
            .option(ChannelOption.TCP_NODELAY, true);

    if (context.getSetting(RECEIVE_BUFFER_SIZE, RECEIVE_BUFFER_SIZE_DEFAULT) != RECEIVE_BUFFER_SIZE_DEFAULT) {
      bootstrap.option(ChannelOption.SO_RCVBUF, context.getSetting(RECEIVE_BUFFER_SIZE, int.class));
    }
    if (context.getSetting(SEND_BUFFER_SIZE, SEND_BUFFER_SIZE_DEFAULT) != SEND_BUFFER_SIZE_DEFAULT) {
      bootstrap.option(ChannelOption.SO_SNDBUF, context.getSetting(SEND_BUFFER_SIZE, int.class));
    }

    return bootstrap;
  }

  private static void startup(ServerConnection serverConnection, BasicContext context) throws IOException, NoticeException {

    Map<String, Object> params = new HashMap<>();
    params.put(APPLICATION_NAME, context.getSetting(APPLICATION_NAME, APPLICATION_NAME_DEFAULT));
    params.put(CLIENT_ENCODING, context.getSetting(CLIENT_ENCODING, CLIENT_ENCODING_DEFAULT));
    params.put(DATABASE, context.getSetting(DATABASE, ""));
    params.put(CREDENTIALS_USERNAME, context.getSetting(CREDENTIALS_USERNAME, ""));

    CountDownLatch startupCompleted = new CountDownLatch(1);
    AtomicReference<Throwable> startupError = new AtomicReference<>();

    serverConnection.submit(new StartupRequest(params, new StartupRequest.CompletionHandler() {
      @Override
      public String authenticateClear() {
        return context.getSetting(CREDENTIALS_PASSWORD, "");
      }

      @Override
      public String authenticateMD5(byte[] salt) {

        String username = context.getSetting(CREDENTIALS_USERNAME).toString();
        String password = context.getSetting(CREDENTIALS_PASSWORD).toString();

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
      public ByteBuf authenticateGSSorSSPIContinue(ByteBuf data) {
        throw new IllegalStateException("Unsupported Authentication Method");
      }

      @Override
      public void handleComplete(Integer processId, Integer secretKey, Map<String, String> parameterStatuses, List<Notice> notices) {

        context.setKeyData(processId, secretKey);
        parameterStatuses.forEach(context::updateSystemParameter);

        startupCompleted.countDown();
      }

      @Override
      public void handleError(Throwable error, List<Notice> notices) {
        startupError.set(error);
        startupCompleted.countDown();
      }

    }));

    if (!awaitUninterruptibly(DEFAULT_STARTUP_TIMEOUT, SECONDS, startupCompleted::await)) {
      throw new IOException("Timeout starting connection");
    }

    if (startupError.get() != null) {
      Throwable error = startupError.get();
      if (error instanceof IOException) throw (IOException) error;
      if (error instanceof NoticeException) throw (NoticeException) error;
      if (error instanceof RuntimeException) throw (RuntimeException) error;
      throw new RuntimeException(error);
    }

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
      else {
        io = new SSLException(io.getCause().getMessage(), io.getCause());
      }
    }

    if (io instanceof SSLException) {
      if (!io.getMessage().startsWith("SSL Error"))
        io = new SSLException("SSL Error: " + io.getMessage(), io.getCause());
    }

    return io;
  }

  static class DefaultHandler implements ParameterStatus, ReportNotice, Notification {

    private WeakReference<BasicContext> context;

    DefaultHandler(BasicContext context) {
      this.context = new WeakReference<>(context);
    }

    private BasicContext getContext() {
      return context.get();
    }

    @Override
    public String toString() {
      return "DEFAULT";
    }

    @Override
    public Action parameterStatus(String name, String value) {
      getContext().updateSystemParameter(name, value);
      return Action.Resume;
    }

    @Override
    public void notification(int processId, String channelName, String payload) {
      getContext().reportNotification(processId, channelName, payload);
    }

    @Override
    public void exception(Throwable cause) {
    }

    @Override
    public Action notice(Notice notice) {
      return null;
    }

  }

}
