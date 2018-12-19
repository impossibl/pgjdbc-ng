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
public class DateTest {

  private Connection con;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "testdate", "dt date");
  }

  @After
  public void after() throws Exception {
    TestUtil.dropTable(con, "testdate");
    TestUtil.closeDB(con);
  }

  /*
   * Tests the time methods in ResultSet
   */
  @Test
  public void testGetDate() throws SQLException {
    Statement stmt = con.createStatement();

    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1950-02-07'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1970-06-02'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1999-08-11'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2001-02-13'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1950-04-02'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1970-11-30'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1988-01-01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2003-07-09'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1934-02-28'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1969-04-03'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1982-08-03'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2012-03-15'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1912-05-01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1971-12-15'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'1984-12-03'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'2000-01-01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'3456-01-01'")));
    assertEquals(1, stmt.executeUpdate(TestUtil.insertSQL("testdate", "'0101-01-01 BC'")));

    /* dateTest() contains all of the tests */
    dateTest();

    assertEquals(18, stmt.executeUpdate("DELETE FROM " + "testdate"));
    stmt.close();
  }

  /*
   * Tests the time methods in PreparedStatement
   */
  @Test
  public void testSetDate() throws SQLException {
    Statement stmt = con.createStatement();
    PreparedStatement ps = con.prepareStatement(TestUtil.insertSQL("testdate", "?"));

    ps.setDate(1, makeDate(1950, 2, 7));
    assertEquals(1, ps.executeUpdate());

    ps.setDate(1, makeDate(1970, 6, 2));
    assertEquals(1, ps.executeUpdate());

    ps.setDate(1, makeDate(1999, 8, 11));
    assertEquals(1, ps.executeUpdate());

    ps.setDate(1, makeDate(2001, 2, 13));
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Timestamp.valueOf("1950-04-02 12:00:00"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Timestamp.valueOf("1970-11-30 3:00:00"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Timestamp.valueOf("1988-01-01 13:00:00"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Timestamp.valueOf("2003-07-09 12:00:00"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "1934-02-28", java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "1969-04-03", java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "1982-08-03", java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "2012-03-15", java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Date.valueOf("1912-05-01"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Date.valueOf("1971-12-15"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Date.valueOf("1984-12-03"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Date.valueOf("2000-01-01"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, java.sql.Date.valueOf("3456-01-01"), java.sql.Types.DATE);
    assertEquals(1, ps.executeUpdate());

    // We can't use valueOf on BC dates.
    ps.setObject(1, makeDate(-100, 1, 1));
    assertEquals(1, ps.executeUpdate());

    ps.close();

    dateTest();

    assertEquals(18, stmt.executeUpdate("DELETE FROM testdate"));
    stmt.close();
  }

  @Test
  public void testSetNull() throws SQLException {
    try (Statement ignored = con.createStatement()) {
      try (PreparedStatement ps = con.prepareStatement(TestUtil.insertSQL("testdate", "?"))) {
        ps.setDate(1, null);
        ps.execute();
      }
    }

  }

  /*
   * Helper for the date tests. It tests what should be in the db
   */
  private void dateTest() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs;
    java.sql.Date d;

    rs = st.executeQuery(TestUtil.selectSQL("testdate", "dt"));
    assertNotNull(rs);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1950, 2, 7), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1970, 6, 2), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1999, 8, 11), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2001, 2, 13), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1950, 4, 2), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1970, 11, 30), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1988, 1, 1), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2003, 7, 9), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1934, 2, 28), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1969, 4, 3), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1982, 8, 3), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2012, 3, 15), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1912, 5, 1), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1971, 12, 15), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(1984, 12, 3), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(2000, 1, 1), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(3456, 1, 1), d);

    assertTrue(rs.next());
    d = rs.getDate(1);
    assertNotNull(d);
    assertEquals(makeDate(-100, 1, 1), d);

    assertTrue(!rs.next());

    rs.close();
    st.close();
  }

  @SuppressWarnings("deprecation")
  private java.sql.Date makeDate(int y, int m, int d) {
    return new java.sql.Date(y - 1900, m - 1, d);
  }
}
