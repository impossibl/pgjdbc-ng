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
package com.impossibl.postgres.jdbc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;

import io.netty.channel.unix.DomainSocketAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * Tests the dynamically created class PGDriver
 *
 */
@RunWith(JUnit4.class)
public class DriverTest {

  /*
   * This tests the acceptsURL() method with a couple of well and poorly formed
   * jdbc urls.
   */
  @Test
  public void testAcceptsURL() throws Exception {
    // Load the driver (note clients should never do it this way!)
    PGDriver drv = new PGDriver();
    assertNotNull(drv);

    // These are always correct
    verifyUrl(drv, "jdbc:pgsql:test", "test", "localhost", 5432);
    verifyUrl(drv, "jdbc:pgsql://localhost/test", "test", "localhost", 5432);
    verifyUrl(drv, "jdbc:pgsql://localhost:5432/test", "test",  "localhost", 5432);
    verifyUrl(drv, "jdbc:pgsql://127.0.0.1/anydbname", "anydbname", "127.0.0.1", 5432);
    verifyUrl(drv, "jdbc:pgsql://127.0.0.1:5433/hidden", "hidden", "127.0.0.1", 5433);
    verifyUrl(drv, "jdbc:pgsql://[::1]:5740/db", "db", "0:0:0:0:0:0:0:1", 5740);
    verifyUrl(drv, "jdbc:pgsql:test?unixsocket=/tmp/.s.PGSQL.5432", "test", "/tmp/.s.PGSQL.5432", null);

    // Badly formatted url's
    assertTrue(!drv.acceptsURL("jdbc:postgres:test"));
    assertTrue(!drv.acceptsURL("postgresql:test"));
    assertTrue(!drv.acceptsURL("db"));
    assertTrue(!drv.acceptsURL("jdbc:pgsql://localhost:5432a/test"));

    // failover urls
    verifyUrl(drv, "jdbc:pgsql://localhost,127.0.0.1:5432/test", "test", "localhost", 5432, "127.0.0.1", 5432);
    verifyUrl(drv, "jdbc:pgsql://localhost:5433,127.0.0.1:5432/test", "test", "localhost", 5433, "127.0.0.1", 5432);
    verifyUrl(drv, "jdbc:pgsql://[::1],[::1]:5432/db", "db", "0:0:0:0:0:0:0:1", 5432, "0:0:0:0:0:0:0:1", 5432);
    verifyUrl(drv, "jdbc:pgsql://[::1]:5740,127.0.0.1:5432/db", "db", "0:0:0:0:0:0:0:1", 5740, "127.0.0.1", 5432);
    verifyUrl(drv, "jdbc:pgsql://localhost,127.0.0.1:5432/test?unixsocket=/tmp/.s.PGSQL.5432", "test", "/tmp/.s.PGSQL.5432", null, "localhost", 5432, "127.0.0.1", 5432);
  }

  private void verifyUrl(PGDriver drv, String url, String dbName, Object... hosts) throws Exception {
    assertTrue(url, drv.acceptsURL(url));
    ConnectionUtil.ConnectionSpecifier connSpec = ConnectionUtil.parseURL(url);
    assertEquals(url, dbName, connSpec.getDatabase());
    assertEquals(url, hosts.length / 2, connSpec.getAddresses().size());
    for (int c = 0; c < hosts.length / 2; ++c) {
      SocketAddress addr = connSpec.getAddresses().get(c);
      if (addr instanceof InetSocketAddress) {
        assertEquals(url, hosts[c * 2], ((InetSocketAddress) addr).getHostString());
        assertEquals(url, hosts[c * 2 + 1], ((InetSocketAddress) addr).getPort());
      }
      else if (addr instanceof DomainSocketAddress) {
        assertEquals(url, hosts[c * 2], ((DomainSocketAddress) addr).path());
      }
      else {
        fail("Unknown socket address: " + addr);
      }
    }
  }

  /*
   * Tests parseURL (internal)
   */
  /*
   * Tests the connect method by connecting to the test database
   */
  @Test
  public void testConnect() throws Exception {
    // Test with the url, username & password
    Connection con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    con.close();

    // Test with the username in the url
    con = DriverManager.getConnection(TestUtil.getURL("user", TestUtil.getUser(), "password", TestUtil.getPassword()));
    assertNotNull(con);
    con.close();

    // Test with failover url
    String url = "jdbc:pgsql://invalidhost.not.here," + TestUtil.getServer() + ":" + TestUtil.getPort() + "/" + TestUtil.getDatabase();
    con = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    con.close();

  }

  /*
   * Test that the readOnly property works.
   */
  @Test
  public void testReadOnly() throws Exception {
    Connection con = DriverManager.getConnection(TestUtil.getURL("readOnly", true), TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    assertTrue(con.isReadOnly());
    con.close();

    con = DriverManager.getConnection(TestUtil.getURL("readOnly", false), TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    assertFalse(con.isReadOnly());
    con.close();

    con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    assertFalse(con.isReadOnly());
    con.close();
  }

  /*
   * Test that the parsedSqlCacheSize property works.
   */
  @Test
  public void testParsedSqlCacheSize() throws Exception {
    ConnectionUtil.ConnectionSpecifier connSpec = ConnectionUtil.parseURL("jdbc:pgsql://localhost/test?parsedSqlCacheSize=100");
    assertSame(100, JDBCSettings.PARSED_SQL_CACHE_SIZE.get(connSpec.getParameters()));
  }

  /*
   * Test that the networkTimeout property works.
   */
  @Test
  public void testNetworkTimeout() throws Exception {
    ConnectionUtil.ConnectionSpecifier connSpec = ConnectionUtil.parseURL("jdbc:pgsql://localhost/test?networkTimeout=10000");
    assertEquals((Integer) 10000, JDBCSettings.DEFAULT_NETWORK_TIMEOUT.get(connSpec.getParameters()));
  }

}
