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

import com.impossibl.postgres.api.data.Interval;
import com.impossibl.postgres.types.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;



@RunWith(JUnit4.class)
public class IntervalTest {

  private Connection _conn;

  @Before
  public void before() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "testinterval", "v interval");
    TestUtil.createTable(_conn, "testdate", "v date");
  }

  @After
  public void after() throws Exception {
    TestUtil.dropTable(_conn, "testinterval");
    TestUtil.dropTable(_conn, "testdate");
    TestUtil.closeDB(_conn);
  }

  @Test
  public void testOnlineTests() throws SQLException {
    PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO testinterval VALUES (?)");
    pstmt.setObject(1, new Interval(2004, 13, 28, 0, 0, 43000.9013));
    pstmt.executeUpdate();
    pstmt.close();

    Statement stmt = _conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT v FROM testinterval");
    assertTrue(rs.next());
    Interval pgi = (Interval) rs.getObject(1);
    assertEquals(2005, pgi.getYears());
    assertEquals(1, pgi.getMonths());
    assertEquals(28, pgi.getDays());
    assertEquals(11, pgi.getHours());
    assertEquals(56, pgi.getMinutes());
    assertEquals(40.9013, pgi.getSeconds(), 0.000001);
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testStringToIntervalCoercion() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-01'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-02'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-04'"));
    stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2010-01-05'"));
    stmt.close();

    PreparedStatement pstmt = _conn
        .prepareStatement("SELECT v FROM testdate WHERE v < (?::timestamp with time zone + ? * ?::interval) ORDER BY v");
    pstmt.setObject(1, makeDate(2010, 1, 1));
    pstmt.setObject(2, Integer.valueOf(2));
    pstmt.setObject(3, "1 day");
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
  public void testIntervalToStringCoercion() throws SQLException {
    Interval interval = new Interval("1 year 3 months");
    PGConnectionImpl pgConnectionImpl = _conn.unwrap(PGConnectionImpl.class);
    Type type = pgConnectionImpl.getRegistry().loadType("Interval");
    String coercedStringValue = SQLTypeUtils.coerceToString(interval, type, pgConnectionImpl);

    assertEquals("@ 1 years 3 months 0 days 0 hours 0 minutes 0.000000 seconds", coercedStringValue);
  }

  @Test(expected = SQLException.class)
  public void testIntervalCoercionFailure() throws SQLException {
    SQLTypeUtils.coerceToInterval(Boolean.FALSE);
  }

  @Test
  public void testDaysHours() throws SQLException {
    Statement stmt = _conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '101:12:00'::interval");
    assertTrue(rs.next());
    Interval i = (Interval) rs.getObject(1);
    assertEquals(0, i.getDays());
    assertEquals(101, i.getHours());
    assertEquals(12, i.getMinutes());
  }

  @Test
  public void testAddRounding() {
    Interval pgi = new Interval(0, 0, 0, 0, 0, 0.6006);
    Calendar cal = Calendar.getInstance();
    long origTime = cal.getTime().getTime();
    pgi.addTo(cal);
    long newTime = cal.getTime().getTime();
    assertEquals(601, newTime - origTime);
    pgi.setSeconds(-0.6006);
    pgi.addTo(cal);
    assertEquals(origTime, cal.getTime().getTime());
  }

  @Test
  public void testOfflineTests() throws Exception {
    Interval pgi = new Interval(2004, 4, 20, 15, 57, 12.1);

    assertEquals(2004, pgi.getYears());
    assertEquals(4, pgi.getMonths());
    assertEquals(20, pgi.getDays());
    assertEquals(15, pgi.getHours());
    assertEquals(57, pgi.getMinutes());
    assertEquals(12.1, pgi.getSeconds(), 0);

    Interval pgi2 = new Interval("@ 2004 years 4 mons 20 days 15 hours 57 mins 12.1 secs");
    assertEquals(pgi, pgi2);

    // Singular units
    Interval pgi3 = new Interval("@ 2004 year 4 mon 20 day 15 hour 57 min 12.1 sec");
    assertEquals(pgi, pgi3);

    Interval pgi4 = new Interval("2004 years 4 mons 20 days 15:57:12.1");
    assertEquals(pgi, pgi4);

    // Ago test
    pgi = new Interval("@ 2004 years 4 mons 20 days 15 hours 57 mins 12.1 secs ago");
    assertEquals(-2004, pgi.getYears());
    assertEquals(-4, pgi.getMonths());
    assertEquals(-20, pgi.getDays());
    assertEquals(-15, pgi.getHours());
    assertEquals(-57, pgi.getMinutes());
    assertEquals(-12.1, pgi.getSeconds(), 0);

    // Char test
    pgi = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs");
    assertEquals(2003, pgi.getYears());
    assertEquals(8, pgi.getMonths());
    assertEquals(20, pgi.getDays());
    assertEquals(-14, pgi.getHours());
    assertEquals(-03, pgi.getMinutes());
    assertEquals(-12.1, pgi.getSeconds(), 0);
  }

  Calendar getStartCalendar() {
    Calendar cal = new GregorianCalendar();
    cal.set(Calendar.YEAR, 2005);
    cal.set(Calendar.MONTH, 4);
    cal.set(Calendar.DAY_OF_MONTH, 29);
    cal.set(Calendar.HOUR_OF_DAY, 15);
    cal.set(Calendar.MINUTE, 35);
    cal.set(Calendar.SECOND, 42);
    cal.set(Calendar.MILLISECOND, 100);

    return cal;
  }

  @Test
  public void testCalendar() throws Exception {
    Calendar cal = getStartCalendar();

    Interval pgi = new Interval("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs");
    pgi.addTo(cal);

    assertEquals(2006, cal.get(Calendar.YEAR));
    assertEquals(5, cal.get(Calendar.MONTH));
    assertEquals(30, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(16, cal.get(Calendar.HOUR_OF_DAY));
    assertEquals(36, cal.get(Calendar.MINUTE));
    assertEquals(43, cal.get(Calendar.SECOND));
    assertEquals(100, cal.get(Calendar.MILLISECOND));

    pgi = new Interval("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs ago");
    pgi.addTo(cal);

    assertEquals(2005, cal.get(Calendar.YEAR));
    assertEquals(4, cal.get(Calendar.MONTH));
    assertEquals(29, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(15, cal.get(Calendar.HOUR_OF_DAY));
    assertEquals(35, cal.get(Calendar.MINUTE));
    assertEquals(42, cal.get(Calendar.SECOND));
    assertEquals(100, cal.get(Calendar.MILLISECOND));

    cal = getStartCalendar();

    pgi = new Interval("@ 1 year -23 hours -3 mins -3.30 secs");
    pgi.addTo(cal);

    assertEquals(2006, cal.get(Calendar.YEAR));
    assertEquals(4, cal.get(Calendar.MONTH));
    assertEquals(28, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(16, cal.get(Calendar.HOUR_OF_DAY));
    assertEquals(32, cal.get(Calendar.MINUTE));
    assertEquals(38, cal.get(Calendar.SECOND));
    assertEquals(800, cal.get(Calendar.MILLISECOND));

    pgi = new Interval("@ 1 year -23 hours -3 mins -3.30 secs ago");
    pgi.addTo(cal);

    assertEquals(2005, cal.get(Calendar.YEAR));
    assertEquals(4, cal.get(Calendar.MONTH));
    assertEquals(29, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(15, cal.get(Calendar.HOUR_OF_DAY));
    assertEquals(35, cal.get(Calendar.MINUTE));
    assertEquals(42, cal.get(Calendar.SECOND));
    assertEquals(100, cal.get(Calendar.MILLISECOND));
  }

  @Test
  public void testDate() throws Exception {
    Date date = getStartCalendar().getTime();
    Date date2 = getStartCalendar().getTime();

    Interval pgi = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs");
    pgi.addTo(date);

    Interval pgi2 = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs ago");
    pgi2.addTo(date);

    assertEquals(date2, date);
  }

  @Test
  public void testISODate() throws Exception {
    Date date = getStartCalendar().getTime();
    Date date2 = getStartCalendar().getTime();

    Interval pgi = new Interval("+2004 years -4 mons +20 days -15:57:12.1");
    pgi.addTo(date);

    Interval pgi2 = new Interval("-2004 years 4 mons -20 days 15:57:12.1");
    pgi2.addTo(date);

    assertEquals(date2, date);
  }

  @SuppressWarnings("deprecation")
  private java.sql.Date makeDate(int y, int m, int d) {
    return new java.sql.Date(y - 1900, m - 1, d);
  }

}
