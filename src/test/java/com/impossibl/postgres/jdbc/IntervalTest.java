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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import com.impossibl.postgres.data.Interval;



public class IntervalTest extends TestCase {

  private Connection _conn;

  public IntervalTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "testinterval", "v interval");
  }

  protected void tearDown() throws Exception {
    TestUtil.dropTable(_conn, "testinterval");
    TestUtil.closeDB(_conn);
  }

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

  public void testDaysHours() throws SQLException {
    Statement stmt = _conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '101:12:00'::interval");
    assertTrue(rs.next());
    Interval i = (Interval) rs.getObject(1);
    assertEquals(0, i.getDays());
    assertEquals(101, i.getHours());
    assertEquals(12, i.getMinutes());
  }

//  public void testAddRounding() {
//    Interval pgi = new Interval(0, 0, 0, 0, 0, 0.6006);
//    Calendar cal = Calendar.getInstance();
//    long origTime = cal.getTime().getTime();
//    pgi.add(cal);
//    long newTime = cal.getTime().getTime();
//    assertEquals(601, newTime - origTime);
//    pgi.setSeconds(-0.6006);
//    pgi.add(cal);
//    assertEquals(origTime, cal.getTime().getTime());
//  }
//
//  public void testOfflineTests() throws Exception {
//    Interval pgi = new Interval(2004, 4, 20, 15, 57, 12.1);
//
//    assertEquals(2004, pgi.getYears());
//    assertEquals(4, pgi.getMonths());
//    assertEquals(20, pgi.getDays());
//    assertEquals(15, pgi.getHours());
//    assertEquals(57, pgi.getMinutes());
//    assertEquals(12.1, pgi.getSeconds(), 0);
//
//    Interval pgi2 = new Interval("@ 2004 years 4 mons 20 days 15 hours 57 mins 12.1 secs");
//    assertEquals(pgi, pgi2);
//
//    // Singular units
//    Interval pgi3 = new Interval("@ 2004 year 4 mon 20 day 15 hour 57 min 12.1 sec");
//    assertEquals(pgi, pgi3);
//
//    Interval pgi4 = new Interval("2004 years 4 mons 20 days 15:57:12.1");
//    assertEquals(pgi, pgi4);
//
//    // Ago test
//    pgi = new Interval("@ 2004 years 4 mons 20 days 15 hours 57 mins 12.1 secs ago");
//    assertEquals(-2004, pgi.getYears());
//    assertEquals(-4, pgi.getMonths());
//    assertEquals(-20, pgi.getDays());
//    assertEquals(-15, pgi.getHours());
//    assertEquals(-57, pgi.getMinutes());
//    assertEquals(-12.1, pgi.getSeconds(), 0);
//
//    // Char test
//    pgi = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs");
//    assertEquals(2004, pgi.getYears());
//    assertEquals(-4, pgi.getMonths());
//    assertEquals(20, pgi.getDays());
//    assertEquals(-15, pgi.getHours());
//    assertEquals(57, pgi.getMinutes());
//    assertEquals(-12.1, pgi.getSeconds(), 0);
//  }
//
//  Calendar getStartCalendar() {
//    Calendar cal = new GregorianCalendar();
//    cal.set(Calendar.YEAR, 2005);
//    cal.set(Calendar.MONTH, 4);
//    cal.set(Calendar.DAY_OF_MONTH, 29);
//    cal.set(Calendar.HOUR_OF_DAY, 15);
//    cal.set(Calendar.MINUTE, 35);
//    cal.set(Calendar.SECOND, 42);
//    cal.set(Calendar.MILLISECOND, 100);
//
//    return cal;
//  }
//
//  public void testCalendar() throws Exception {
//    Calendar cal = getStartCalendar();
//
//    Interval pgi = new Interval("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs");
//    pgi.add(cal);
//
//    assertEquals(2006, cal.get(Calendar.YEAR));
//    assertEquals(5, cal.get(Calendar.MONTH));
//    assertEquals(30, cal.get(Calendar.DAY_OF_MONTH));
//    assertEquals(16, cal.get(Calendar.HOUR_OF_DAY));
//    assertEquals(36, cal.get(Calendar.MINUTE));
//    assertEquals(43, cal.get(Calendar.SECOND));
//    assertEquals(100, cal.get(Calendar.MILLISECOND));
//
//    pgi = new Interval("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs ago");
//    pgi.add(cal);
//
//    assertEquals(2005, cal.get(Calendar.YEAR));
//    assertEquals(4, cal.get(Calendar.MONTH));
//    assertEquals(29, cal.get(Calendar.DAY_OF_MONTH));
//    assertEquals(15, cal.get(Calendar.HOUR_OF_DAY));
//    assertEquals(35, cal.get(Calendar.MINUTE));
//    assertEquals(42, cal.get(Calendar.SECOND));
//    assertEquals(100, cal.get(Calendar.MILLISECOND));
//
//    cal = getStartCalendar();
//
//    pgi = new Interval("@ 1 year -23 hours -3 mins -3.30 secs");
//    pgi.add(cal);
//
//    assertEquals(2006, cal.get(Calendar.YEAR));
//    assertEquals(4, cal.get(Calendar.MONTH));
//    assertEquals(28, cal.get(Calendar.DAY_OF_MONTH));
//    assertEquals(16, cal.get(Calendar.HOUR_OF_DAY));
//    assertEquals(32, cal.get(Calendar.MINUTE));
//    assertEquals(38, cal.get(Calendar.SECOND));
//    assertEquals(800, cal.get(Calendar.MILLISECOND));
//
//    pgi = new Interval("@ 1 year -23 hours -3 mins -3.30 secs ago");
//    pgi.add(cal);
//
//    assertEquals(2005, cal.get(Calendar.YEAR));
//    assertEquals(4, cal.get(Calendar.MONTH));
//    assertEquals(29, cal.get(Calendar.DAY_OF_MONTH));
//    assertEquals(15, cal.get(Calendar.HOUR_OF_DAY));
//    assertEquals(35, cal.get(Calendar.MINUTE));
//    assertEquals(42, cal.get(Calendar.SECOND));
//    assertEquals(100, cal.get(Calendar.MILLISECOND));
//  }
//
//  public void testDate() throws Exception {
//    Date date = getStartCalendar().getTime();
//    Date date2 = getStartCalendar().getTime();
//
//    Interval pgi = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs");
//    pgi.add(date);
//
//    Interval pgi2 = new Interval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs ago");
//    pgi2.add(date);
//
//    assertEquals(date2, date);
//  }
//
//  public void testISODate() throws Exception {
//    Date date = getStartCalendar().getTime();
//    Date date2 = getStartCalendar().getTime();
//
//    Interval pgi = new Interval("+2004 years -4 mons +20 days -15:57:12.1");
//    pgi.add(date);
//
//    Interval pgi2 = new Interval("-2004 years 4 mons -20 days 15:57:12.1");
//    pgi2.add(date);
//
//    assertEquals(date2, date);
//  }

}
