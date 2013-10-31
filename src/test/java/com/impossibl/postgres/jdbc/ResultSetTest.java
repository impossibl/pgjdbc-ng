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
import java.sql.Types;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

/*
 * ResultSet tests.
 */
@RunWith(JUnit4.class)
public class ResultSetTest {
  private Connection con;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();
    Statement stmt = con.createStatement();

    TestUtil.createTable(con, "testrs", "id integer");

    stmt.executeUpdate("INSERT INTO testrs VALUES (1)");
    stmt.executeUpdate("INSERT INTO testrs VALUES (2)");
    stmt.executeUpdate("INSERT INTO testrs VALUES (3)");
    stmt.executeUpdate("INSERT INTO testrs VALUES (4)");
    stmt.executeUpdate("INSERT INTO testrs VALUES (6)");
    stmt.executeUpdate("INSERT INTO testrs VALUES (9)");

    TestUtil.createTable(con, "teststring", "a text");
    stmt.executeUpdate("INSERT INTO teststring VALUES ('12345')");

    TestUtil.createTable(con, "testint", "a int");
    stmt.executeUpdate("INSERT INTO testint VALUES (12345)");

    TestUtil.createTable(con, "testbool", "a boolean");

    // TestUtil.createTable(con, "testbit", "a bit");

    TestUtil.createTable(con, "testboolstring", "a varchar(30)");

    stmt.executeUpdate("INSERT INTO testboolstring VALUES('true')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('false')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('t')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('f')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('1.0')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('0.0')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('TRUE')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('this is not true')");

    TestUtil.createTable(con, "testnumeric", "a numeric");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('1.0')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('0.0')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-1.0')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('1.2')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-2.5')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('99999.2')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('99999')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-99999.2')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-99999')");

    // Integer.MaxValue
    stmt.execute("INSERT INTO testnumeric VALUES('2147483647')");

    // Integer.MinValue
    stmt.execute("INSERT INTO testnumeric VALUES('-2147483648')");

    stmt.executeUpdate("INSERT INTO testnumeric VALUES('2147483648')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-2147483649')");

    // Long.MaxValue
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('9223372036854775807')");

    // Long.MinValue
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-9223372036854775808')");

    stmt.executeUpdate("INSERT INTO testnumeric VALUES('9223372036854775808')");
    stmt.executeUpdate("INSERT INTO testnumeric VALUES('-9223372036854775809')");

    stmt.close();

  }

  @After
  public void after() throws SQLException {
    TestUtil.dropTable(con, "testrs");
    TestUtil.dropTable(con, "teststring");
    TestUtil.dropTable(con, "testint");
    TestUtil.dropTable(con, "testbool");
    // TestUtil.dropTable(con, "testbit");
    TestUtil.dropTable(con, "testboolstring");
    TestUtil.dropTable(con, "testnumeric");
    TestUtil.closeDB(con);
  }

  @Test
  public void testBackward() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM testrs");
    rs.afterLast();
    assertTrue(rs.previous());
    rs.close();
    stmt.close();
  }

  @Test
  public void testAbsolute() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM testrs");

    assertTrue(!rs.absolute(0));
    assertEquals(0, rs.getRow());

    assertTrue(rs.absolute(-1));
    assertEquals(6, rs.getRow());

    assertTrue(rs.absolute(1));
    assertEquals(1, rs.getRow());

    assertTrue(!rs.absolute(-10));
    assertEquals(0, rs.getRow());
    assertTrue(rs.next());
    assertEquals(1, rs.getRow());

    assertTrue(!rs.absolute(10));
    assertEquals(0, rs.getRow());
    assertTrue(rs.previous());
    assertEquals(6, rs.getRow());

    stmt.close();
  }

  @Test
  public void testEmptyResult() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = stmt.executeQuery("SELECT * FROM testrs where id=100");
    rs.beforeFirst();
    rs.afterLast();
    assertTrue(!rs.first());
    assertTrue(!rs.last());
    assertTrue(!rs.next());
  }

  @Test
  public void testMaxFieldSize() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.setMaxFieldSize(2);

    ResultSet rs = stmt.executeQuery("select * from testint");

    // max should not apply to the following since per the spec
    // it should apply only to binary and char/varchar columns
    rs.next();
    assertEquals("12345", rs.getString(1));
    // getBytes returns 5 bytes for txt transfer, 4 for bin transfer
    assertTrue(rs.getBytes(1).length >= 4);

    // max should apply to the following since the column is
    // a varchar column
    rs = stmt.executeQuery("select * from teststring");
    rs.next();
    assertEquals("12", rs.getString(1));
    assertEquals("12", new String(rs.getBytes(1)));
  }

  @Test
  public void testBoolean() throws SQLException {
    PreparedStatement pstmt = con.prepareStatement("insert into testbool values (?)");

    pstmt.setObject(1, new Float(0), Types.BIT);
    pstmt.executeUpdate();

    pstmt.setObject(1, new Float(1), Types.BIT);
    pstmt.executeUpdate();

    pstmt.setObject(1, "False", Types.BIT);
    pstmt.executeUpdate();

    pstmt.setObject(1, "True", Types.BIT);
    pstmt.executeUpdate();

    ResultSet rs = con.createStatement().executeQuery("select * from testbool");
    for (int i = 0; i < 2; i++) {
      assertTrue(rs.next());
      assertEquals(false, rs.getBoolean(1));
      assertTrue(rs.next());
      assertEquals(true, rs.getBoolean(1));
    }

    /*
     * pstmt = con.prepareStatement("insert into testbit values (?)");
     *
     * pstmt.setObject(1, new Float(0), Types.BIT);
     * pstmt.executeUpdate();
     *
     * pstmt.setObject(1, new Float(1), Types.BIT);
     * pstmt.executeUpdate();
     *
     * pstmt.setObject(1, "false", Types.BIT); pstmt.executeUpdate();
     *
     * pstmt.setObject(1, "true", Types.BIT); pstmt.executeUpdate();
     *
     * rs = con.createStatement().executeQuery("select * from testbit");
     *
     * for (int i = 0;i<2; i++) { assertTrue(rs.next()); assertEquals(false,
     * rs.getBoolean(1)); assertTrue(rs.next()); assertEquals(true,
     * rs.getBoolean(1)); }
     */

    rs = con.createStatement().executeQuery("select * from testboolstring");

    for (int i = 0; i < 4; i++) {
      assertTrue(rs.next());
      assertEquals(true, rs.getBoolean(1));
      assertTrue(rs.next());
      assertEquals(false, rs.getBoolean(1));
    }
  }

  @Test
  public void testgetByte() throws SQLException {
    ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

    assertTrue(rs.next());
    assertEquals(1, rs.getByte(1));

    assertTrue(rs.next());
    assertEquals(0, rs.getByte(1));

    assertTrue(rs.next());
    assertEquals(-1, rs.getByte(1));

    assertTrue(rs.next());
    assertEquals(1, rs.getByte(1));

    assertTrue(rs.next());
    assertEquals(-2, rs.getByte(1));

    while (rs.next()) {
      try {
        rs.getByte(1);
        fail("Exception expected.");
      }
      catch (Exception e) {
        // Ok
      }
    }
    rs.close();
  }

  @Test
  public void testgetShort() throws SQLException {
    ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

    assertTrue(rs.next());
    assertEquals(1, rs.getShort(1));

    assertTrue(rs.next());
    assertEquals(0, rs.getShort(1));

    assertTrue(rs.next());
    assertEquals(-1, rs.getShort(1));

    assertTrue(rs.next());
    assertEquals(1, rs.getShort(1));

    assertTrue(rs.next());
    assertEquals(-2, rs.getShort(1));

    while (rs.next()) {
      try {
        rs.getShort(1);
        fail("Exception expected.");
      }
      catch (Exception e) {
        // Ok
      }
    }
    rs.close();
  }

  @Test
  public void testgetInt() throws SQLException {
    ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(-1, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(-2, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(99999, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(99999, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(-99999, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(-99999, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(Integer.MAX_VALUE, rs.getInt(1));

    assertTrue(rs.next());
    assertEquals(Integer.MIN_VALUE, rs.getInt(1));

    while (rs.next()) {
      try {
        rs.getInt(1);
        fail("Exception expected." + rs.getString(1));
      }
      catch (Exception e) {
        // Ok
      }
    }
    rs.close();
  }

  @Test
  public void testgetLong() throws SQLException {
    ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

    assertTrue(rs.next());
    assertEquals(1, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(0, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(-1, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(1, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(-2, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(99999, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(99999, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(-99999, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(-99999, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(((long) Integer.MAX_VALUE), rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(((long) Integer.MIN_VALUE), rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(((long) Integer.MAX_VALUE) + 1, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(((long) Integer.MIN_VALUE) - 1, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(Long.MAX_VALUE, rs.getLong(1));

    assertTrue(rs.next());
    assertEquals(Long.MIN_VALUE, rs.getLong(1));

    while (rs.next()) {
      try {
        rs.getLong(1);
        fail("Exception expected." + rs.getString(1));
      }
      catch (Exception e) {
        // Ok
      }
    }
    rs.close();
  }

  @Test
  public void testParameters() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    stmt.setFetchSize(100);
    stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN);

    ResultSet rs = stmt.executeQuery("SELECT * FROM testrs");

    assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
    assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stmt.getResultSetType());
    assertEquals(100, stmt.getFetchSize());
    assertEquals(ResultSet.FETCH_UNKNOWN, stmt.getFetchDirection());

//TODO: reconcile against mainstream driver
//    assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
//TODO: reconcile against mainstream driver
//    assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
    assertEquals(100, rs.getFetchSize());
//TODO: reconcile against mainstream driver
//    assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());

    rs.close();
    stmt.close();
  }

  @Test
  public void testZeroRowResultPositioning() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = stmt.executeQuery("SELECT * FROM pg_database WHERE datname='nonexistantdatabase'");
    assertTrue(!rs.previous());
    assertTrue(!rs.previous());
    assertTrue(!rs.next());
    assertTrue(!rs.next());
    assertTrue(!rs.next());
    assertTrue(!rs.next());
    assertTrue(!rs.next());
    assertTrue(!rs.previous());
    assertTrue(!rs.first());
    assertTrue(!rs.last());
    assertEquals(0, rs.getRow());
    assertTrue(!rs.absolute(1));
    assertTrue(!rs.relative(1));
    assertTrue(!rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(!rs.isFirst());
    assertTrue(!rs.isLast());
    rs.close();
    stmt.close();
  }

  @Test
  public void testRowResultPositioning() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    // Create a one row result set.
    ResultSet rs = stmt.executeQuery("SELECT datname FROM pg_database WHERE datname='template1'");

    assertTrue(rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(!rs.isFirst());
    assertTrue(!rs.isLast());

    assertTrue(rs.next());

    assertTrue(!rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(rs.isFirst());
    assertTrue(rs.isLast());

    assertTrue(!rs.next());

    assertTrue(!rs.isBeforeFirst());
    assertTrue(rs.isAfterLast());
    assertTrue(!rs.isFirst());
    assertTrue(!rs.isLast());

    assertTrue(rs.previous());

    assertTrue(!rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(rs.isFirst());
    assertTrue(rs.isLast());

    assertTrue(rs.absolute(1));

    assertTrue(!rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(rs.isFirst());
    assertTrue(rs.isLast());

    assertTrue(!rs.absolute(0));

    assertTrue(rs.isBeforeFirst());
    assertTrue(!rs.isAfterLast());
    assertTrue(!rs.isFirst());
    assertTrue(!rs.isLast());

    assertTrue(!rs.absolute(2));

    assertTrue(!rs.isBeforeFirst());
    assertTrue(rs.isAfterLast());
    assertTrue(!rs.isFirst());
    assertTrue(!rs.isLast());

    rs.close();
    stmt.close();
  }

  @Test
  public void testForwardOnlyExceptions() throws SQLException {
    // Test that illegal operations on a TYPE_FORWARD_ONLY resultset
    // correctly result in throwing an exception.
    Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3); //Forces result set to be forward only
    ResultSet rs = stmt.executeQuery("SELECT * FROM testnumeric");

    try {
      rs.absolute(1);
      fail("absolute() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.afterLast();
      fail("afterLast() on a TYPE_FORWARD_ONLY resultset did not throw an exception on a TYPE_FORWARD_ONLY resultset");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.beforeFirst();
      fail("beforeFirst() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.first();
      fail("first() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.last();
      fail("last() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.previous();
      fail("previous() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.relative(1);
      fail("relative() on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }

    try {
      rs.setFetchDirection(ResultSet.FETCH_REVERSE);
      fail("setFetchDirection(FETCH_REVERSE) on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }

    try {
      rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
      fail("setFetchDirection(FETCH_UNKNOWN) on a TYPE_FORWARD_ONLY resultset did not throw an exception");
    }
    catch (SQLException e) {
      // Ok
    }

    rs.close();
    stmt.close();
  }

  @Test
  public void testCaseInsensitiveFindColumn() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id, id AS \"ID2\" FROM testrs");
    assertEquals(1, rs.findColumn("id"));
    assertEquals(1, rs.findColumn("ID"));
    assertEquals(1, rs.findColumn("Id"));
    assertEquals(2, rs.findColumn("id2"));
    assertEquals(2, rs.findColumn("ID2"));
    assertEquals(2, rs.findColumn("Id2"));
    try {
      rs.findColumn("id3");
      fail("There isn't an id3 column in the ResultSet.");
    }
    catch (SQLException sqle) {
      // Ok
    }
  }

  @Test
  public void testGetOutOfBounds() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM testrs");
    assertTrue(rs.next());

    try {
      rs.getInt(-9);
    }
    catch (SQLException sqle) {
      // Ok
    }

    try {
      rs.getInt(1000);
    }
    catch (SQLException sqle) {
      // Ok
    }
  }

  @Test
  public void testClosedResult() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = stmt.executeQuery("SELECT id FROM testrs");
    rs.close();

    rs.close(); // Closing twice is allowed.
    try {
      rs.getInt(1);
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.getInt("id");
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.getType();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.wasNull();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.absolute(3);
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.isBeforeFirst();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.setFetchSize(10);
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.getMetaData();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.rowUpdated();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.updateInt(1, 1);
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.moveToInsertRow();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    try {
      rs.clearWarnings();
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
  }

  /*
   * The JDBC spec says when you have duplicate column names, the first one
   * should be returned.
   */
  @Test
  public void testDuplicateColumnNameOrder() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS a");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt("a"));
  }

  @Test
  public void testTurkishLocale() throws SQLException {
    Locale current = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT id FROM testrs");
      int sum = 0;
      while (rs.next()) {
        sum += rs.getInt("ID");
      }
      rs.close();
      assertEquals(25, sum);
    }
    finally {
      Locale.setDefault(current);
    }
  }

}