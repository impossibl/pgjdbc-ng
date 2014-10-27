/**
 * Copyright (c) 2014, impossibl.com
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "mactest", "mac_address macaddr, cidr_mask cidr");
  }

  @After
  public void after() throws SQLException {
    TestUtil.dropTable(conn, "mactest");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testMacStringConversion() throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      int rows = stmt.executeUpdate("INSERT into mactest(mac_address) VALUES ('08:00:2b:01:02:03')");
      assertEquals("Number of inserted rows not as expected", 1, rows);

      ResultSet resultSet = stmt.executeQuery("SELECT mac_address FROM mactest WHERE mac_address='08:00:2b:01:02:03'");
      assertTrue(resultSet.next());
      assertEquals("08:00:2b:01:02:03", resultSet.getString(1));
    }
  }

  @Test
  public void testMacPreparedStatement() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("INSERT into mactest(mac_address) VALUES (?)")) {
      stmt.setString(1, "08:00:2b:01:02:03");
      int rows = stmt.executeUpdate();
      assertEquals("Number of inserted rows not as expected", 1, rows);
    }
  }

  @Test
  public void testMacBatch() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("INSERT into mactest(mac_address) VALUES (CAST (? AS macaddr))")) {
      stmt.setString(1, "08:00:2b:01:02:03");
      stmt.addBatch();
      stmt.setString(1, "08-00-2b-01-02-03");
      stmt.addBatch();
      stmt.setString(1, "08002b:010203");
      stmt.addBatch();
      stmt.setString(1, "08002b-010203");
      stmt.addBatch();
      stmt.setString(1, "0800.2b01.0203");
      stmt.addBatch();
      stmt.setString(1, "08002b010203");
      stmt.addBatch();
      int[] batchResult = stmt.executeBatch();
      assertEquals("Number of inserted rows not as expected", 6, batchResult.length);
      for (int rows : batchResult) {
        assertEquals("Number of inserted rows not as expected", 1, rows);
      }
    }
  }

  @Test
  public void testCidrStringConversion() throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      int rows = stmt.executeUpdate("INSERT into mactest(cidr_mask) VALUES ('192.168/24')");
      assertEquals("Number of inserted rows not as expected", 1, rows);

      ResultSet resultSet = stmt.executeQuery("SELECT cidr_mask FROM mactest WHERE cidr_mask='192.168.0.0/24'");
      assertTrue(resultSet.next());
      assertEquals("192.168.0.0/24", resultSet.getString(1));
    }
  }

  @Test
  public void testCidrPreparedStatement() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("INSERT into mactest(cidr_mask) VALUES (?)")) {
      stmt.setString(1, "192.168.100.128/25");
      int rows = stmt.executeUpdate();
      assertEquals("Number of inserted rows not as expected", 1, rows);
    }
  }

  @Test
  public void testCidrBatch() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("INSERT into mactest(cidr_mask) VALUES (CAST (? AS cidr))")) {
      stmt.setString(1, "192.168/25");
      stmt.addBatch();
      stmt.setString(1, "192.168.1");
      stmt.addBatch();
      stmt.setString(1, "128");
      stmt.addBatch();
      stmt.setString(1, "2001:4f8:3:ba::/64");
      stmt.addBatch();
      stmt.setString(1, "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128");
      stmt.addBatch();
      stmt.setString(1, "::ffff:1.2.3.0/120");
      stmt.addBatch();
      int[] batchResult = stmt.executeBatch();
      assertEquals("Number of inserted rows not as expected", 6, batchResult.length);
      for (int rows : batchResult) {
        assertEquals("Number of inserted rows not as expected", 1, rows);
      }
    }
  }
}
