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

import com.impossibl.postgres.api.data.Interval;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Period;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IntervalTest {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "testinterval", "v interval");
    TestUtil.createTable(conn, "testdate", "v date");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(conn, "testinterval");
    TestUtil.dropTable(conn, "testdate");

    TestUtil.closeDB(conn);

    System.clearProperty("pgjdbc.param.format");
    System.clearProperty("pgjdbc.field.format");
  }

  @Test
  public void testOnlinePostgresStyle() throws SQLException {

    try (Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("SET INTERVALSTYLE = 'postgres'"));

      PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
      pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
      pstmt.executeUpdate();
      pstmt.close();

      try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
        assertTrue(rs.next());
        Interval pgi = (Interval) rs.getObject(1);
        assertEquals(2005, pgi.getPeriod().getYears());
        assertEquals(1, pgi.getPeriod().getMonths());
        assertEquals(28, pgi.getPeriod().getDays());
        assertEquals(43000, pgi.getDuration().getSeconds());
        assertEquals(901_300_000, pgi.getDuration().getNano());
        assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
        assertFalse(rs.next());
      }
      try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
        assertTrue(rs.next());
        assertEquals("2005 years 1 mon 28 days 11:56:40.9013", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testOnlinePostgresStyleText() throws SQLException {
    System.setProperty("pgjdbc.param.format", "text");
    System.setProperty("pgjdbc.field.format", "text");

    Properties props = new Properties();
    props.setProperty(JDBCSettings.REGISTRY_SHARING.getName(), "false");

    try (Connection conn = TestUtil.openDB(props)) {
      try (Statement stmt = conn.createStatement()) {
        assertFalse(stmt.execute("SET INTERVALSTYLE = 'postgres'"));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
        pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
        pstmt.executeUpdate();
        pstmt.close();

        try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
          assertTrue(rs.next());
          Interval pgi = (Interval) rs.getObject(1);
          assertEquals(2005, pgi.getPeriod().getYears());
          assertEquals(1, pgi.getPeriod().getMonths());
          assertEquals(28, pgi.getPeriod().getDays());
          assertEquals(43000, pgi.getDuration().getSeconds());
          assertEquals(901_300_000, pgi.getDuration().getNano());
          assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
          assertFalse(rs.next());
        }
        try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
          assertTrue(rs.next());
          assertEquals("2005 years 1 mon 28 days 11:56:40.9013", rs.getString(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testOnlinePostgresVerboseStyle() throws SQLException {

    try (Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("SET INTERVALSTYLE = 'postgres_verbose'"));

      PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
      pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
      pstmt.executeUpdate();
      pstmt.close();

      try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
        assertTrue(rs.next());
        Interval pgi = (Interval) rs.getObject(1);
        assertEquals(2005, pgi.getPeriod().getYears());
        assertEquals(1, pgi.getPeriod().getMonths());
        assertEquals(28, pgi.getPeriod().getDays());
        assertEquals(43000, pgi.getDuration().getSeconds());
        assertEquals(901_300_000, pgi.getDuration().getNano());
        assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
        assertFalse(rs.next());
      }
      try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
        assertTrue(rs.next());
        assertEquals("@ 2005 years 1 mon 28 days 11 hours 56 mins 40.9013 secs", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testOnlinePostgresVerboseStyleText() throws SQLException {
    System.setProperty("pgjdbc.param.format", "text");
    System.setProperty("pgjdbc.field.format", "text");

    Properties props = new Properties();
    props.setProperty(JDBCSettings.REGISTRY_SHARING.getName(), "false");

    try (Connection conn = TestUtil.openDB(props)) {
      try (Statement stmt = conn.createStatement()) {
        assertFalse(stmt.execute("SET INTERVALSTYLE = 'postgres_verbose'"));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
        pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
        pstmt.executeUpdate();
        pstmt.close();

        try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
          assertTrue(rs.next());
          Interval pgi = (Interval) rs.getObject(1);
          assertEquals(2005, pgi.getPeriod().getYears());
          assertEquals(1, pgi.getPeriod().getMonths());
          assertEquals(28, pgi.getPeriod().getDays());
          assertEquals(43000, pgi.getDuration().getSeconds());
          assertEquals(901_300_000, pgi.getDuration().getNano());
          assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
          assertFalse(rs.next());
        }
        try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
          assertTrue(rs.next());
          assertEquals("@ 2005 years 1 mon 28 days 11 hours 56 mins 40.9013 secs", rs.getString(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testOnlineISOStyle() throws SQLException {

    try (Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("SET INTERVALSTYLE = 'iso_8601'"));

      PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
      pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
      pstmt.executeUpdate();
      pstmt.close();

      try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
        assertTrue(rs.next());
        Interval pgi = (Interval) rs.getObject(1);
        assertEquals(2005, pgi.getPeriod().getYears());
        assertEquals(1, pgi.getPeriod().getMonths());
        assertEquals(28, pgi.getPeriod().getDays());
        assertEquals(43000, pgi.getDuration().getSeconds());
        assertEquals(901_300_000, pgi.getDuration().getNano());
        assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
        assertFalse(rs.next());
      }
      try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
        assertTrue(rs.next());
        assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testOnlineISOStyleText() throws SQLException {
    System.setProperty("pgjdbc.param.format", "text");
    System.setProperty("pgjdbc.field.format", "text");

    Properties props = new Properties();
    props.setProperty(JDBCSettings.REGISTRY_SHARING.getName(), "false");

    try (Connection conn = TestUtil.openDB(props)) {
      try (Statement stmt = conn.createStatement()) {
        assertFalse(stmt.execute("SET INTERVALSTYLE = 'iso_8601'"));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
        pstmt.setObject(1, Interval.of(Period.of(2004, 13, 28), Duration.ofSeconds(43000, 901_300_000)));
        pstmt.executeUpdate();
        pstmt.close();

        try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
          assertTrue(rs.next());
          Interval pgi = (Interval) rs.getObject(1);
          assertEquals(2005, pgi.getPeriod().getYears());
          assertEquals(1, pgi.getPeriod().getMonths());
          assertEquals(28, pgi.getPeriod().getDays());
          assertEquals(43000, pgi.getDuration().getSeconds());
          assertEquals(901_300_000, pgi.getDuration().getNano());
          assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
          assertFalse(rs.next());
        }
        try (ResultSet rs = stmt.executeQuery("SELECT v::text FROM testinterval")) {
          assertTrue(rs.next());
          assertEquals("P2005Y1M28DT11H56M40.9013S", rs.getString(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testStringToIntervalCoercion() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-01'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-02'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-04'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-05'"));
    stmt.close();

    PreparedStatement pstmt =
        conn.prepareStatement("SELECT v FROM testdate WHERE v < (?::timestamp with time zone + ? * ?::interval) ORDER BY v");
    pstmt.setObject(1, makeDate(2010, 1, 1));
    pstmt.setObject(2, Integer.valueOf(2));
    pstmt.setObject(3, "P1D");
    ResultSet rs = pstmt.executeQuery();

    assertNotNull(rs);

    java.sql.Date d;

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2010, 1, 1), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2010, 1, 2), d);

    assertFalse(rs.next());

    rs.close();
    pstmt.close();
  }

  @Test
  public void testStringToIntervalCoercion2() throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO testinterval(v) VALUES(?::interval)")) {
      ps.setString(1, "PT12345678.234567S");
      ps.executeUpdate();
    }
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval")) {
        assertTrue(rs.next());
        assertEquals(Interval.of(Duration.ofSeconds(12345678, 234_567_000)), rs.getObject(1));
      }
    }
  }

  @Test
  public void testDaysHours() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '101:12:00'::interval");
    assertTrue(rs.next());
    Interval i = (Interval) rs.getObject(1);
    assertEquals(0, i.getPeriod().getDays());
    assertEquals(101, i.getDuration().toHours());
    assertEquals(12, i.getDuration().minusHours(i.getDuration().toHours()).toMinutes());
  }

  @SuppressWarnings("deprecation")
  private java.sql.Date makeDate(int y, int m, int d) {
    return new java.sql.Date(y - 1900, m - 1, d);
  }

}
