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
package com.impossibl.postgres.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class CiTextTest {

  private static final String ASSUMPTION = "Extension `citext` is installed";

  private Connection conn;
  private boolean ciTextInstalled;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    ciTextInstalled = TestUtil.isExtensionInstalled(conn, "citext");
    if (ciTextInstalled) {
      TestUtil.createTable(conn, "users", "name text, email citext");
    }
  }

  @After
  public void after() throws SQLException {
    if (ciTextInstalled) {
      TestUtil.dropTable(conn, "users");
    }
    TestUtil.closeDB(conn);
  }

  @Test
  public void testCiTextInSchema() throws SQLException {
    assumeTrue("testnoexts database is created", TestUtil.isDatabaseCreated("testnoexts"));

    String url = "jdbc:pgsql://" + TestUtil.getServer() + ":" + TestUtil.getPort() + "/testnoexts";

    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());


    try (Connection conn = DriverManager.getConnection(url, props)) {

      try {

        try (Statement stmt = conn.createStatement()) {
          stmt.execute("CREATE SCHEMA citester");
          stmt.execute("CREATE EXTENSION citext WITH SCHEMA citester");
          stmt.execute("CREATE TABLE citester.test(names citester.citext[])");
        }

        try (Connection conn2 = DriverManager.getConnection(url, props)) {

          try (PreparedStatement stmt = conn2.prepareStatement("INSERT INTO citester.test(names) VALUES (?::citester.citext[])")) {

            stmt.setObject(1, new String[] {"test"}, JDBCType.ARRAY);

            assertEquals(stmt.executeUpdate(), 1);
          }

          try (Statement stmt = conn2.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM citester.test")) {

              assertTrue(rs.next());
              assertArrayEquals(rs.getObject("names", String[].class), new String[] {"test"});

            }
          }
        }

      }
      finally {
        TestUtil.dropSchema(conn, "citester");
      }

    }

  }

  @Test
  public void testCiTextInPlainQuery() throws SQLException {
    assumeTrue(ASSUMPTION, ciTextInstalled);

    try (Statement stmt = conn.createStatement()) {
      int rows = stmt.executeUpdate("INSERT INTO users(name, email) VALUES ('Rich Drake', 'RDrake@gmail.com')");
      assertEquals(1, rows);

      ResultSet rs = stmt.executeQuery("SELECT name, email FROM users WHERE email='rdrake@gmail.com'");
      assertTrue(rs.next());
      assertEquals("Rich Drake", rs.getString(1));
      assertEquals("RDrake@gmail.com", rs.getString(2));
    }
  }

  @Test
  public void testCiTextInPreparedStatement() throws SQLException {
    assumeTrue(ASSUMPTION, ciTextInstalled);

    try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO users(name, email) VALUES (?, ?)")) {
      stmt.setString(1, "Fred Miller");
      stmt.setString(2, "Fred-Miller@outlook.com");
      int rows = stmt.executeUpdate();
      assertEquals(1, rows);
    }
  }

  @Test
  public void testCiTextInBatch() throws SQLException {
    assumeTrue(ASSUMPTION, ciTextInstalled);

    try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO users(name, email) VALUES (?, ?)")) {
      stmt.setString(1, "Lisa Meyer");
      stmt.setString(2, "lisa@meyer.me");
      stmt.addBatch();
      stmt.setString(1, "Jim Porter");
      stmt.setString(2, "JIMPORTER@bigcorp.org");
      stmt.addBatch();
      stmt.setString(1, "Sara Eakins");
      stmt.setString(2, "sara_eakins@inbox.com");
      stmt.addBatch();
      int[] batchResult = stmt.executeBatch();
      assertEquals(3, batchResult.length);
      for (int rows : batchResult) {
        assertEquals(1, rows);
      }
    }
  }
}
