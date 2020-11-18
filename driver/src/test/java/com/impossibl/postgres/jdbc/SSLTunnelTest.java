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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.jdbc.util.TcpProxyServer;

import static com.impossibl.postgres.jdbc.TestUtil.getDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSLTunnelTest {

  private SelfSignedCertificate serverCert;
  private SelfSignedCertificate clientCert;
  private TcpProxyServer proxyServer;

  @Before
  public void setup() throws Exception {
    serverCert = new SelfSignedCertificate();
    clientCert = new SelfSignedCertificate();
    SslContext proxySslContext = SslContextBuilder.forServer(serverCert.key(), serverCert.cert()).build();
    proxyServer = new TcpProxyServer(proxySslContext, TestUtil.getServer(), Integer.parseInt(TestUtil.getPort()));
    proxyServer.start();
  }

  @After
  public void teardown() throws Exception {
    serverCert.delete();
    clientCert.delete();
    proxyServer.shutdownGracefully();
  }

  @Test
  public void canConnect() throws Exception {
    String query = TestUtil.getQuery(
        "sslMode", "tunnel",
        "sslCertificateFile", clientCert.certificate().getAbsolutePath(),
        "sslKeyFile", clientCert.privateKey().getAbsolutePath(),
        "sslRootCertificateFile", serverCert.certificate().getAbsolutePath()
    );
    String url = "jdbc:pgsql://localhost:" + proxyServer.port() + "/" + getDatabase() + query;
    try (Connection conn = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select 1")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }
      }
    }
  }

}
