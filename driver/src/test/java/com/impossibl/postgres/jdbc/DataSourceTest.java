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

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.system.Setting;
import com.impossibl.postgres.system.Settings;

import static com.impossibl.postgres.jdbc.DataSourceSettings.DS;
import static com.impossibl.postgres.jdbc.DataSourceSettings.LOCAL_SERVER_NAME;
import static com.impossibl.postgres.jdbc.DataSourceSettings.SERVER_ADDRESSES;
import static com.impossibl.postgres.jdbc.DataSourceSettings.SERVER_NAME;
import static com.impossibl.postgres.jdbc.JDBCSettings.JDBC;
import static com.impossibl.postgres.system.SystemSettings.PROTO;
import static com.impossibl.postgres.system.SystemSettings.SYS;
import static com.impossibl.postgres.utils.StringTransforms.toLowerCamelCase;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import io.netty.channel.unix.DomainSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for PGDataSource
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
@RunWith(JUnit4.class)
public class DataSourceTest {

  private Connection con;

  @Before
  public void before() throws Exception {
    PGDataSource ds = new PGDataSource();
    ds.setServerName(TestUtil.getServer());
    ds.setPortNumber(Integer.valueOf(TestUtil.getPort()));
    ds.setDatabaseName(TestUtil.getDatabase());
    ds.setUser(TestUtil.getUser());
    ds.setPassword(TestUtil.getPassword());
    ds.setNetworkTimeout(10000);

    con = ds.getConnection();
  }

  @After
  public void after() throws Exception {
    TestUtil.closeDB(con);
  }

  /*
   * Test getConnection()
   */
  @Test
  public void testGetConnection() throws Exception {
    assertNotNull(con);
    assertTrue(con instanceof PGConnection);
    assertTrue(con.isValid(5));
  }

  /*
   * Test getConnection() parameters don't persist in DataSource
   */
  @Test
  public void testGetConnectionDoesntOverride() {
    String user = Long.toString(new Random().nextLong());
    String pass = Long.toString(new Random().nextLong());
    PGDataSource ds = new PGDataSource();
    ds.setServerName(TestUtil.getServer());
    ds.setPortNumber(Integer.valueOf(TestUtil.getPort()));
    ds.setDatabaseName(TestUtil.getDatabase());
    ds.setUser(user);
    ds.setPassword(pass);
    ds.setNetworkTimeout(10000);

    try (Connection con = ds.getConnection(TestUtil.getUser(), TestUtil.getPassword())) {
      assertThat(con, instanceOf(PGConnection.class));
    }
    catch (Exception e) {
      fail("Couldn't connect with test credentials");
    }

    // Verify DataSource's (invalid) credentials weren't changed
    assertEquals(ds.getUser(), user);
    assertEquals(ds.getPassword(), pass);
  }

  @Test
  public void testConnectionSpecifierBuild() throws SQLException {
    Settings settings = new Settings();
    settings.set(SERVER_NAME, "example.com");

    // No default for local server
    ConnectionUtil.ConnectionSpecifier connSpec = PGDataSource.buildConnectionSpecifier(settings);
    assertEquals(singletonList(new InetSocketAddress("example.com", 5432)), connSpec.getAddresses());

    // Local server name should come first
    settings.set(LOCAL_SERVER_NAME, "/var/run/pg");
    connSpec = PGDataSource.buildConnectionSpecifier(settings);

    assertEquals(asList(new DomainSocketAddress("/var/run/pg"), new InetSocketAddress("example.com", 5432)), connSpec.getAddresses());

    // Server addresses overrides other properties
    settings.set(SERVER_ADDRESSES, "1.2.3.4:2345, example.com");
    connSpec = PGDataSource.buildConnectionSpecifier(settings);

    assertEquals(asList(new InetSocketAddress("1.2.3.4", 2345), new InetSocketAddress("example.com", 5432)), connSpec.getAddresses());
  }

  @Test
  public void testServerAddressParsing() throws SQLException {
    assertEquals(PGDataSource.parseServerAddress("/tmp"), new DomainSocketAddress("/tmp"));
    assertEquals(PGDataSource.parseServerAddress("[::1]:2345"), new InetSocketAddress("::1", 2345));
    assertEquals(PGDataSource.parseServerAddress("[::1]"), new InetSocketAddress("::1", 5432));
    assertEquals(PGDataSource.parseServerAddress("1.2.3.4:2345"), new InetSocketAddress("1.2.3.4", 2345));
    assertEquals(PGDataSource.parseServerAddress("1.2.3.4"), new InetSocketAddress("1.2.3.4", 5432));

    assertTrue(parseAddressFails("[]"));
    assertTrue(parseAddressFails("[::1]:"));
    assertTrue(parseAddressFails("[]:123"));
    assertTrue(parseAddressFails("[::1]:abc"));
    assertTrue(parseAddressFails("[::1]2345"));
    assertTrue(parseAddressFails("[::1"));
    assertTrue(parseAddressFails("1.2.3.4:"));
    assertTrue(parseAddressFails("1.2.3.4:abc"));
    assertTrue(parseAddressFails(":2345"));
    assertTrue(parseAddressFails(":"));
  }

  boolean parseAddressFails(String address) {
    try {
      PGDataSource.parseServerAddress(address);
      return false;
    }
    catch (SQLException e) {
      return true;
    }
  }

  /*
   * Test setNetworkTimeout()
   */
  @Test
  public void testSetNetworkTimeout() throws Exception {
    assertEquals(10000, con.getNetworkTimeout());
  }

  @Test
  public void testSettingsAreValidProperties() throws Exception {
    PGDataSource ds = new PGDataSource();
    BeanInfo beanInfo = Introspector.getBeanInfo(PGDataSource.class);

    for (Setting<?> setting : new Settings(JDBC, DS, SYS, PROTO).knownSet()) {

      PropertyDescriptor settingPD = findPropertyDescriptor(beanInfo, toLowerCamelCase(setting.getName()));

      assertNotNull("Missing property for setting " + setting.getName() + " (" + toLowerCamelCase(setting.getName()) + ")", settingPD);
    }

  }

  private PropertyDescriptor findPropertyDescriptor(BeanInfo beanInfo, String name) {
    for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
      if (pd.getName().equals(name)) {
        return pd;
      }
    }
    return null;
  }

}
