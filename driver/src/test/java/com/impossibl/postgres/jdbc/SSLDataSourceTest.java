/**
 * Copyright (c) 2015, impossibl.com
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

import static com.impossibl.postgres.jdbc.TestUtil.isDatabaseCreated;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;


@RunWith(Parameterized.class)
public class SSLDataSourceTest {

  @BeforeClass
  public static void checkDbsExist() throws SQLException {
    assumeTrue("Missing hostnossldb", isDatabaseCreated("hostnossldb"));
    assumeTrue("Missing hostdb", isDatabaseCreated("hostdb"));
    assumeTrue("Missing hostssldb", isDatabaseCreated("hostssldb"));
    assumeTrue("Missing hostsslcertdb", isDatabaseCreated("hostsslcertdb"));
    assumeTrue("Missing certdb", isDatabaseCreated("certdb"));
  }

  protected String certdir;
  protected String db;
  protected String sslmode;
  protected boolean goodclient;
  protected boolean goodserver;
  protected String prefix;
  protected Object[] expected;

  public SSLDataSourceTest(String name, String certdir, String db, String sslmode,
                           boolean goodclient, boolean goodserver,
                           String prefix, Object[] expected) {
    this.certdir = certdir;
    this.db = db;
    this.sslmode = sslmode;
    this.goodclient = goodclient;
    this.goodserver = goodserver;
    this.prefix = prefix;
    this.expected = expected;
  }

  @Test
  public void testConnection() throws Throwable {
    driver(makeDataSource(), expected);
  }

  private DataSource makeDataSource() {
    PGDataSource ds = new PGDataSource();
    ds.setServerName("localhost");
    ds.setPortNumber(Integer.valueOf(TestUtil.getPort()));

    if ("sslhostnossl".equals(db)) {
      ds.setDatabaseName("hostnossldb");
    }
    else if ("sslhostgh".equals(db)) {
      ds.setDatabaseName("hostdb");
    }
    else if ("sslhostbh".equals(db)) {
      ds.setServerName("127.0.0.1");
      ds.setDatabaseName("hostdb");
    }
    else if ("sslhostsslgh".equals(db)) {
      ds.setDatabaseName("hostssldb");
    }
    else if ("sslhostsslbh".equals(db)) {
      ds.setServerName("127.0.0.1");
      ds.setDatabaseName("hostssldb");
    }
    else if ("sslhostsslcertgh".equals(db)) {
      ds.setDatabaseName("hostsslcertdb");
    }
    else if ("sslhostsslcertbh".equals(db)) {
      ds.setServerName("127.0.0.1");
      ds.setDatabaseName("hostsslcertdb");
    }
    else if ("sslcertgh".equals(db)) {
      ds.setDatabaseName("certdb");
    }
    else if ("sslcertbh".equals(db)) {
      ds.setServerName("127.0.0.1");
      ds.setDatabaseName("certdb");
    }

    ds.setUser(TestUtil.getUser());
    ds.setPassword(TestUtil.getPassword());
    ds.setNetworkTimeout(10000);

    ds.setSslMode(sslmode);
    ds.setSslKeyPassword("sslpwd");
    ds.setSslCertificateFile(certdir + "/" + prefix + (goodclient ? "goodclient.crt" : "badclient.crt"));
    ds.setSslKeyFile(certdir + "/" + prefix + (goodclient ? "goodclient.pk8" : "badclient.pk8"));
    ds.setSslCaCertificateFile(certdir + "/" + prefix + (goodserver ? "goodroot.crt" : "badroot.crt"));

    return ds;
  }

  /**
   * Tries to connect to the database.
   *
   * @param ds
   *          DataSource instance
   * @param expected
   *          Expected values. the first element is a String holding the
   *          expected message of PSQLException or null, if no exception is
   *          expected, the second indicates weather ssl is to be used (Boolean)
   * @throws SQLException
   */
  protected void driver(DataSource ds, Object[] expected) throws SQLException {
    String exmsg = (String) expected[0];
    try {
      try (Connection conn = ds.getConnection()) {
        if (exmsg != null) {
          fail("Exception did not occur: " + exmsg);
        }
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery("select ssl_is_used()")) {
            assertTrue(rs.next());
            assertEquals("ssl_is_used: ", ((Boolean) expected[1]).booleanValue(), rs.getBoolean(1));
          }
        }
      }
    }
    catch (SQLException ex) {
      if (exmsg == null) { // no exception is excepted
        fail("Exception thrown: " + ex.getMessage());
      }
      else {
        assertTrue("expected: " + exmsg + " actual: " + ex.getMessage(), ex.getMessage().matches(exmsg));
        return;
      }
    }
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> suite() throws Exception {
    Collection<Object[]> data = new ArrayList<>();

    Properties prop = new Properties();
    prop.load(new FileInputStream("src/test/resources/ssltest.properties"));
    add(data, prop, "ssloff");
    add(data, prop, "sslhostnossl");

    String[] hostmode = {"sslhost", "sslhostssl", "sslhostsslcert", "sslcert"};
    String[] certmode = {"gh", "bh"};

    for (int i = 0; i < hostmode.length; i++) {
      for (int j = 0; j < certmode.length; j++) {
        add(data, prop, hostmode[i] + certmode[j]);
      }
    }

    return data;
  }

  private static void add(Collection<Object[]> data, Properties prop, String param) {

    if (prop.getProperty(param, "").equals("")) {
      System.out.println("Skipping " + param + ".");
    }
    else {
      data.addAll(testData(prop, param));
    }

  }

  private static Collection<Object[]> testData(Properties prop, String param) {
    String certdir = prop.getProperty("certdir");
    String sprefix = prop.getProperty(param + "prefix");
    String[] csslmode = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"};

    Map<String, Object[]> expected = expectedmap.get(param);
    if (expected == null) {
      expected = defaultexpected;
    }

    Collection<Object[]> data = new ArrayList<>();

    for (int i = 0; i < csslmode.length; i++) {
      data.add(new Object[] {param + "-" + csslmode[i] + "GG", certdir, param, csslmode[i], true, true, sprefix, expected.get(csslmode[i] + "GG")});
      data.add(new Object[] {param + "-" + csslmode[i] + "GB", certdir, param, csslmode[i], true, false, sprefix, expected.get(csslmode[i] + "GB")});
      data.add(new Object[] {param + "-" + csslmode[i] + "BG", certdir, param, csslmode[i], false, true, sprefix, expected.get(csslmode[i] + "BG")});
    }
    return data;
  }

  static Map<String, Map<String, Object[]>> expectedmap;
  static TreeMap<String, Object[]> defaultexpected;

  static String PG_HBA_ON = "Connection Error: no pg_hba.conf entry for host .*, user .*, database .*, SSL on(?s-d:.*)";
  static String PG_HBA_OFF = "Connection Error: no pg_hba.conf entry for host .*, user .*, database .*, SSL off(?s-d:.*)";
  static String FAILED = "The connection attempt failed.";
  static String BROKEN = "Connection Error: SSL Error: Received fatal alert: unknown_ca";
  static String ANY = ".*";
  static String VALIDATOR = "Connection Error: SSL Error: PKIX path (building|validation) failed:.*";
  static String HOSTNAME = "Connection Error: SSL Error: The hostname .* could not be verified";

  static {
    defaultexpected = new TreeMap<>();
    defaultexpected.put("disableGG", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("disableGB", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("disableBG", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("allowGG", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("allowGB", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("allowBG", new Object[] {null, Boolean.FALSE});
    defaultexpected.put("preferGG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("preferGB", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("preferBG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("requireGG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("requireGB", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("requireBG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("verify-caGB", new Object[] {ANY, Boolean.TRUE});
    defaultexpected.put("verify-caBG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("verify-fullGG", new Object[] {null, Boolean.TRUE});
    defaultexpected.put("verify-fullGB", new Object[] {ANY, Boolean.TRUE});
    defaultexpected.put("verify-fullBG", new Object[] {null, Boolean.TRUE});

    expectedmap = new TreeMap<>();

    TreeMap<String, Object[]> work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {null, Boolean.FALSE});
    work.put("disableGB", new Object[] {null, Boolean.FALSE});
    work.put("disableBG", new Object[] {null, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.FALSE});
    work.put("allowGB", new Object[] {null, Boolean.FALSE});
    work.put("allowBG", new Object[] {null, Boolean.FALSE});
    work.put("preferGG", new Object[] {null, Boolean.FALSE});
    work.put("preferGB", new Object[] {null, Boolean.FALSE});
    work.put("preferBG", new Object[] {null, Boolean.FALSE});
    work.put("requireGG", new Object[] {ANY, Boolean.TRUE});
    work.put("requireGB", new Object[] {ANY, Boolean.TRUE});
    work.put("requireBG", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {ANY, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {ANY, Boolean.TRUE});
    expectedmap.put("ssloff", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {null, Boolean.FALSE});
    work.put("disableGB", new Object[] {null, Boolean.FALSE});
    work.put("disableBG", new Object[] {null, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.FALSE});
    work.put("allowGB", new Object[] {null, Boolean.FALSE});
    work.put("allowBG", new Object[] {null, Boolean.FALSE});
    work.put("preferGG", new Object[] {null, Boolean.FALSE});
    work.put("preferGB", new Object[] {null, Boolean.FALSE});
    work.put("preferBG", new Object[] {null, Boolean.FALSE});
    work.put("requireGG", new Object[] {PG_HBA_ON, Boolean.TRUE});
    work.put("requireGB", new Object[] {PG_HBA_ON, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {PG_HBA_ON, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {PG_HBA_ON, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslhostnossl", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {null, Boolean.FALSE});
    work.put("disableGB", new Object[] {null, Boolean.FALSE});
    work.put("disableBG", new Object[] {null, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.FALSE});
    work.put("allowGB", new Object[] {null, Boolean.FALSE});
    work.put("allowBG", new Object[] {null, Boolean.FALSE});
    work.put("preferGG", new Object[] {null, Boolean.TRUE});
    work.put("preferGB", new Object[] {null, Boolean.TRUE});
    work.put("preferBG", new Object[] {null, Boolean.FALSE});
    work.put("requireGG", new Object[] {null, Boolean.TRUE});
    work.put("requireGB", new Object[] {null, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslhostgh", work);

    work = new TreeMap<>(work);
    work.put("disableGG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableGB", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.TRUE});
    work.put("allowGB", new Object[] {null, Boolean.TRUE});
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    expectedmap.put("sslhostsslgh", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {null, Boolean.FALSE});
    work.put("disableGB", new Object[] {null, Boolean.FALSE});
    work.put("disableBG", new Object[] {null, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.FALSE});
    work.put("allowGB", new Object[] {null, Boolean.FALSE});
    work.put("allowBG", new Object[] {null, Boolean.FALSE});
    work.put("preferGG", new Object[] {null, Boolean.TRUE});
    work.put("preferGB", new Object[] {null, Boolean.TRUE});
    work.put("preferBG", new Object[] {null, Boolean.FALSE});
    work.put("requireGG", new Object[] {null, Boolean.TRUE});
    work.put("requireGB", new Object[] {null, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {HOSTNAME, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslhostbh", work);

    work = new TreeMap<>(work);
    work.put("disableGG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableGB", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.TRUE});
    work.put("allowGB", new Object[] {null, Boolean.TRUE});
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.TRUE});
    expectedmap.put("sslhostsslbh", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableGB", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.TRUE});
    work.put("allowGB", new Object[] {null, Boolean.TRUE});
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferGG", new Object[] {null, Boolean.TRUE});
    work.put("preferGB", new Object[] {null, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.TRUE});
    work.put("requireGG", new Object[] {null, Boolean.TRUE});
    work.put("requireGB", new Object[] {null, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslhostsslcertgh", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableGB", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.TRUE});
    work.put("allowGB", new Object[] {null, Boolean.TRUE});
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferGG", new Object[] {null, Boolean.TRUE});
    work.put("preferGB", new Object[] {null, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.TRUE});
    work.put("requireGG", new Object[] {null, Boolean.TRUE});
    work.put("requireGB", new Object[] {null, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {HOSTNAME, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslhostsslcertbh", work);

    work = new TreeMap<>(defaultexpected);
    work.put("disableGG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableGB", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("disableBG", new Object[] {PG_HBA_OFF, Boolean.FALSE});
    work.put("allowGG", new Object[] {null, Boolean.TRUE});
    work.put("allowGB", new Object[] {null, Boolean.TRUE});
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferGG", new Object[] {null, Boolean.TRUE});
    work.put("preferGB", new Object[] {null, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.TRUE});
    work.put("requireGG", new Object[] {null, Boolean.TRUE});
    work.put("requireGB", new Object[] {null, Boolean.TRUE});
    work.put("requireBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-caGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-caGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-caBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {null, Boolean.TRUE});
    work.put("verify-fullGB", new Object[] {VALIDATOR, Boolean.TRUE});
    work.put("verify-fullBG", new Object[] {BROKEN, Boolean.TRUE});
    expectedmap.put("sslcertgh", work);

    work = new TreeMap<>(work);
    work.put("allowBG", new Object[] {BROKEN, Boolean.TRUE});
    work.put("preferBG", new Object[] {PG_HBA_OFF, Boolean.TRUE});
    work.put("verify-fullGG", new Object[] {HOSTNAME, Boolean.TRUE});
    expectedmap.put("sslcertbh", work);
  }

}
