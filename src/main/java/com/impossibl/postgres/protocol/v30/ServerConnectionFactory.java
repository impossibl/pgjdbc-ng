package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ssl.SSLEngineFactory;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.NoticeException;

import static com.impossibl.postgres.system.Settings.APPLICATION_NAME;
import static com.impossibl.postgres.system.Settings.APPLICATION_NAME_DEFAULT;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING_DEFAULT;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE;
import static com.impossibl.postgres.system.Settings.SSL_MODE;
import static com.impossibl.postgres.system.Settings.SSL_MODE_DEFAULT;
import static com.impossibl.postgres.utils.Await.awaitUninterruptibly;
import static com.impossibl.postgres.utils.StringTransforms.capitalizeOption;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
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

  private ServerConnection connect(SSLMode sslMode, final SocketAddress address, BasicContext context) throws IOException, NoticeException {

    try {

      ServerConnectionShared.Ref sharedRef = ServerConnectionShared.acquire();

      Bootstrap clientBootstrap = sharedRef.get().getBootstrap();

      ChannelFuture connectFuture = clientBootstrap.connect(address).syncUninterruptibly();

      ServerConnection channel = new ServerConnection(context, connectFuture.channel(), sharedRef);

      if (sslMode != SSLMode.Disable && sslMode != SSLMode.Allow) {

        // Execute SSL query command

        SSLQueryRequest sslQueryRequest = new SSLQueryRequest();

        channel.submit(sslQueryRequest);

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

        startup(channel, context);

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

      return channel;
    }
    catch (NoticeException e) {

      throw e;
    }
    catch (Exception e) {

      throw translateConnectionException(e);
    }

  }

  private static void startup(ServerConnection channel, BasicContext context) throws IOException, NoticeException {

    Map<String, Object> params = new HashMap<>();
    params.put(APPLICATION_NAME, context.getSetting(APPLICATION_NAME, APPLICATION_NAME_DEFAULT));
    params.put(CLIENT_ENCODING, context.getSetting(CLIENT_ENCODING, CLIENT_ENCODING_DEFAULT));
    params.put(DATABASE, context.getSetting(DATABASE, ""));
    params.put(CREDENTIALS_USERNAME, context.getSetting(CREDENTIALS_USERNAME, ""));

    CountDownLatch startupCompleted = new CountDownLatch(1);
    AtomicReference<Throwable> startupError = new AtomicReference<>();

    channel.submit(new StartupRequest(params, new StartupRequest.CompletionHandler() {

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

}
