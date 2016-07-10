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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/*
 * Some simple tests based on problems reported by users. Hopefully these will
 * help prevent previous problems from re-occuring ;-)
 *
 */
@RunWith(JUnit4.class)
public class TimeTest {

  private Connection con;
  private boolean testSetTime = false;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "testtime", "tm time, tz time with time zone");
  }

  @After
  public void after() throws Exception {
    TestUtil.dropTable(con, "testtime");
    TestUtil.closeDB(con);
  }

  private long extractMillis(long time) {
    return (time >= 0) ? (time % 1000) : (time % 1000 + 1000);
  }

  /*
   *
   * Test use of calendar
   */
  @Test
  public void testGetTimeZone() throws Exception {
    @SuppressWarnings("deprecation")
    final Time midnight = new Time(0, 0, 0);
    Statement stmt = con.createStatement();
    Calendar cal = Calendar.getInstance();

    cal.setTimeZone(TimeZone.getTimeZone("GMT"));

    int localOffset = Calendar.getInstance().getTimeZone().getOffset(midnight.getTime());

    // set the time to midnight to make this easy
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'00:00:00','00:00:00'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'00:00:00.1','00:00:00.01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "CAST(CAST(now() AS timestamp without time zone) AS time),now()")));
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testtime", "tm,tz"));
    assertNotNull(rs);
    assertTrue(rs.next());

    Time time = rs.getTime(1);
    Timestamp timestamp = rs.getTimestamp(1);
    assertNotNull(timestamp);

    Timestamp timestamptz = rs.getTimestamp(2);
    assertNotNull(timestamptz);

    Time timetz = rs.getTime(2);
    assertEquals(midnight, time);

    time = rs.getTime(1, cal);
    assertEquals(midnight.getTime(), time.getTime() - localOffset);

    assertTrue(rs.next());

    time = rs.getTime(1);
    assertNotNull(time);
    assertEquals(100, extractMillis(time.getTime()));
    timestamp = rs.getTimestamp(1);
    assertNotNull(timestamp);

    assertEquals(100, extractMillis(timestamp.getTime()));

    assertEquals(100000000, timestamp.getNanos());

    timetz = rs.getTime(2);
    assertNotNull(timetz);
    assertEquals(10, extractMillis(timetz.getTime()));
    timestamptz = rs.getTimestamp(2);
    assertNotNull(timestamptz);

    assertEquals(10, extractMillis(timestamptz.getTime()));

    assertEquals(10000000, timestamptz.getNanos());

    assertTrue(rs.next());

    time = rs.getTime(1);
    assertNotNull(time);
    timestamp = rs.getTimestamp(1);
    assertNotNull(timestamp);

    timetz = rs.getTime(2);
    assertNotNull(timetz);
    timestamptz = rs.getTimestamp(2);
    assertNotNull(timestamptz);
  }

  /*
   * Tests the time methods in ResultSet
   */
  @Test
  public void testGetTime() throws SQLException {
    Statement stmt = con.createStatement();

    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'01:02:03'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'23:59:59'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'12:00:00'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'05:15:21'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'16:21:51'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'12:15:12'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'22:12:01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testtime", "'08:46:44'")));

    // Fall through helper
    timeTest();

    assertEquals(8, stmt.executeUpdate("DELETE FROM testtime"));
    stmt.close();
  }

  /*
   * Tests the time methods in PreparedStatement
   */
  @Test
  public void testSetTime() throws SQLException {
    PreparedStatement ps = con.prepareStatement(TestUtil.insertSQL("testtime", "?"));
    Statement stmt = con.createStatement();

    ps.setTime(1, makeTime(1, 2, 3));
    assertEquals(1, ps.executeUpdate());

    ps.setTime(1, makeTime(23, 59, 59));
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Time.valueOf("12:00:00"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Time.valueOf("05:15:21"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Time.valueOf("16:21:51"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Time.valueOf("12:15:12"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "22:12:1", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "8:46:44", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "5:1:2-03", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "23:59:59+11", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    // Need to let the test know this one has extra test cases.
    testSetTime = true;
    // Fall through helper
    timeTest();
    testSetTime = false;

    assertEquals(10, stmt.executeUpdate("DELETE FROM testtime"));
    stmt.close();
    ps.close();
  }

  @Test
  public void testSetNull() throws SQLException {
    try (Statement stmt = con.createStatement()) {
      try (PreparedStatement ps = con.prepareStatement(TestUtil.insertSQL("testtime", "?"))) {
        ps.setTime(1, null);
        ps.execute();
      }
    }

  }

  /*
   * Helper for the TimeTests. It tests what should be in the db
   */
  @SuppressWarnings("deprecation")
  private void timeTest() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs;
    java.sql.Time t;

    rs = st.executeQuery(TestUtil.selectSQL("testtime", "tm"));
    assertNotNull(rs);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(1, 2, 3), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(23, 59, 59), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(12, 0, 0), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(5, 15, 21), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(16, 21, 51), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(12, 15, 12), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(22, 12, 1), t);

    assertTrue(rs.next());
    t = rs.getTime(1);
    assertNotNull(t);
    assertEquals(makeTime(8, 46, 44), t);

    // If we're checking for timezones.
    if (testSetTime) {
      assertTrue(rs.next());
      t = rs.getTime(1);
      assertNotNull(t);
      java.sql.Time tmpTime = java.sql.Time.valueOf("5:1:2");
      int localoffset = java.util.Calendar.getInstance().getTimeZone().getOffset(tmpTime.getTime());
      int Timeoffset = 3 * 60 * 60 * 1000;
      tmpTime.setTime(tmpTime.getTime() + Timeoffset + localoffset);
      assertEquals(makeTime(tmpTime.getHours(), tmpTime.getMinutes(), tmpTime.getSeconds()), t);

      assertTrue(rs.next());
      t = rs.getTime(1);
      assertNotNull(t);
      tmpTime = java.sql.Time.valueOf("23:59:59");
      localoffset = java.util.Calendar.getInstance().getTimeZone().getOffset(tmpTime.getTime());
      Timeoffset = -11 * 60 * 60 * 1000;
      tmpTime.setTime(tmpTime.getTime() + Timeoffset + localoffset);
      assertEquals(makeTime(tmpTime.getHours(), tmpTime.getMinutes(), tmpTime.getSeconds()), t);
    }

    assertTrue(!rs.next());

    rs.close();
  }

  private java.sql.Time makeTime(int h, int m, int s) {
    return java.sql.Time.valueOf(TestUtil.fix(h, 2) + ":" + TestUtil.fix(m, 2) + ":" + TestUtil.fix(s, 2));
  }
}
