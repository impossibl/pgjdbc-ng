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

import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.jdbc.util.Asserts.assertThrows;
import static com.impossibl.postgres.utils.guava.ByteStreams.toByteArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    TestUtil.createTable(con, "testbytes", "a bytea");
    stmt.executeUpdate("INSERT INTO testbytes VALUES (convert_to('12345', 'UTF8'))");

    TestUtil.createTable(con, "testint", "a int");
    stmt.executeUpdate("INSERT INTO testint VALUES (12345)");

    TestUtil.createTable(con, "testbool", "a boolean");

    TestUtil.createTable(con, "testbit", "a bit");

    TestUtil.createTable(con, "testboolstring", "a varchar(30)");

    stmt.executeUpdate("INSERT INTO testboolstring VALUES('true')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('false')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('t')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('f')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('on')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('off')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('1')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('0')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('this is not true')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('this is not false')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('1.0')");
    stmt.executeUpdate("INSERT INTO testboolstring VALUES('0.0')");

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
    rs.close();
    stmt.close();
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

    rs.close();

    // max should apply to the following since the column is
    // a varchar column
    rs = stmt.executeQuery("select * from teststring");
    rs.next();
    assertEquals("12", rs.getString(1));
    assertEquals("12", new String(rs.getBytes(1)));

    rs.close();
    stmt.close();
  }

  @Test
  public void testBoolean() throws SQLException {
    {
      PreparedStatement pstmt = con.prepareStatement("INSERT INTO testbool VALUES (?)");

      pstmt.setObject(1, 0f, Types.BIT);
      pstmt.executeUpdate();

      pstmt.setObject(1, 1f, Types.BIT);
      pstmt.executeUpdate();

      pstmt.setObject(1, "False", Types.BIT);
      pstmt.executeUpdate();

      pstmt.setObject(1, "True", Types.BIT);
      pstmt.executeUpdate();

      pstmt.close();
    }

    {
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("SELECT * FROM testbool");

      for (int i = 0; i < 2; i++) {
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
      }

      rs.close();
      st.close();
    }

    {
      PreparedStatement pstmt = con.prepareStatement("INSERT INTO testbit VALUES (?)");

      pstmt.setObject(1, 0f, Types.BIT);
      pstmt.executeUpdate();

      pstmt.setObject(1, 1f, Types.BIT);
      pstmt.executeUpdate();
      pstmt.setObject(1, "0", Types.BIT);
      pstmt.executeUpdate();
      pstmt.setObject(1, "1", Types.BIT);
      pstmt.executeUpdate();
      assertThrows(SQLException.class, () -> pstmt.setObject(1, "false", Types.BIT));
      assertThrows(SQLException.class, () -> pstmt.setObject(1, "true", Types.BIT));

      ResultSet rs = con.createStatement().executeQuery("SELECT * FROM testbit");

      for (int i = 0; i < 2; i++) {
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
      }
    }

    {
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("SELECT * FROM testboolstring");

      for (int i = 0; i < 4; i++) {
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
      }
      for (int i = 0; i < 2; i++) {
        assertTrue(rs.next());
        assertThrows(Exception.class, () -> rs.getBoolean(1));
      }

      rs.close();
      st.close();
    }
  }

  @Test
  public void testgetByte() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testnumeric");

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
    st.close();
  }

  @Test
  public void testgetShort() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testnumeric");

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
    st.close();
  }

  @Test
  public void testgetInt() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testnumeric");

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
    st.close();
  }

  @Test
  public void testgetLong() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testnumeric");

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
    assertEquals((Integer.MAX_VALUE), rs.getLong(1));

    assertTrue(rs.next());
    assertEquals((Integer.MIN_VALUE), rs.getLong(1));

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
    st.close();
  }

  @Test
  public void testgetBytes() throws SQLException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testbytes");

    assertTrue(rs.next());
    assertArrayEquals("12345".getBytes(UTF_8), rs.getBytes(1));
    // Ensure we can call it multiple times with correct result
    assertArrayEquals("12345".getBytes(UTF_8), rs.getBytes(1));

    rs.close();
    st.close();
  }

  @Test
  public void testgetBinaryStream() throws SQLException, IOException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from testbytes");

    assertTrue(rs.next());
    try (InputStream is = rs.getBinaryStream(1)) {
      assertArrayEquals("12345".getBytes(UTF_8), toByteArray(is));
    }
    // Ensure we can call it multiple times with correct result
    try (InputStream is = rs.getBinaryStream(1)) {
      assertArrayEquals("12345".getBytes(UTF_8), toByteArray(is));
    }

    rs.close();
    st.close();
  }

  @Test
  public void testgetAsciiStream() throws SQLException, IOException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from teststring");

    assertTrue(rs.next());
    try (InputStream is = rs.getBinaryStream(1)) {
      assertArrayEquals("12345".getBytes(US_ASCII), toByteArray(is));
    }
    // Ensure we can call it multiple times with correct result
    try (InputStream is = rs.getBinaryStream(1)) {
      assertArrayEquals("12345".getBytes(US_ASCII), toByteArray(is));
    }

    rs.close();
    st.close();
  }

  @Test
  public void testgetCharacterStream() throws SQLException, IOException {
    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select * from teststring");

    assertTrue(rs.next());
    try (Reader r = rs.getCharacterStream(1)) {
      assertEquals("12345", CharStreams.toString(r));
    }
    // Ensure we can call it multiple times with correct result
    try (Reader r = rs.getCharacterStream(1)) {
      assertEquals("12345", CharStreams.toString(r));
    }

    rs.close();
    st.close();
  }

  @Test
  public void testRowId() throws SQLException {

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT ctid, * FROM testrs");
    assertTrue(rs.next());
    assertNotNull(rs.getRowId(1));
    rs.close();
    stmt.close();

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

    assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
    assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
    assertEquals(100, rs.getFetchSize());
    assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());

    rs.close();
    stmt.close();
  }

  @Test
  public void testFetchDirection() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM testrs");
    rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);

    assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());

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

    rs.close();
    stmt.close();
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

    rs.close();
    stmt.close();
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
    stmt.close();
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
    rs.close();
    stmt.close();
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
      stmt.close();
      assertEquals(25, sum);
    }
    finally {
      Locale.setDefault(current);
    }
  }

}
