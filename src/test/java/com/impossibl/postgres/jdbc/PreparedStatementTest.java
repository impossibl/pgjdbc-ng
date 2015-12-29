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

import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.Path;
import com.impossibl.postgres.jdbc.util.BrokenInputStream;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.utils.GeometryParsers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class PreparedStatementTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "streamtable", "bin bytea, str text");
    TestUtil.createTable(conn, "texttable", "ch char(3), te text, vc varchar(3)");
    TestUtil.createTable(conn, "intervaltable", "i interval");
    TestUtil.createTable(conn, "inttable", "a int");
  }

  @After
  public void after() throws SQLException {
    TestUtil.dropTable(conn, "streamtable");
    TestUtil.dropTable(conn, "texttable");
    TestUtil.dropTable(conn, "intervaltable");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testLargeBinaryStream() throws SQLException {

    byte[] data = new byte[Settings.PARAMETER_STREAM_THRESHOLD_DEFAULT * 2];

    ByteArrayInputStream bais = new ByteArrayInputStream(data);

    doSetBinaryStream(bais, data.length);

  }

  @Test
  public void testSetBinaryStream() throws SQLException {
    ByteArrayInputStream bais;
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    bais = null;
    doSetBinaryStream(bais, 0);

    bais = new ByteArrayInputStream(new byte[0]);
    doSetBinaryStream(bais, 0);

    bais = new ByteArrayInputStream(buf);
    doSetBinaryStream(bais, 0);

    bais = new ByteArrayInputStream(buf);
    doSetBinaryStream(bais, 10);
  }

  @Test
  public void testGetBinaryStream() throws SQLException {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(buf);
    doSetBinaryStream(bais, 10);

    ((PGConnectionImpl)conn).setStrictMode(true);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bin FROM streamtable");
    assertTrue(rs.next());
    assertTrue(Arrays.equals(buf, (byte[])rs.getObject(1)));
    rs.close();
    stmt.close();
    ((PGConnectionImpl)conn).setStrictMode(false);
  }

  @Test
  public void testSetAsciiStream() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos, "ASCII"));
    pw.println("Hello");
    pw.flush();

    ByteArrayInputStream bais;

    bais = new ByteArrayInputStream(baos.toByteArray());
    doSetAsciiStream(bais, 0);

    bais = new ByteArrayInputStream(baos.toByteArray());
    doSetAsciiStream(bais, 6);

    bais = new ByteArrayInputStream(baos.toByteArray());
    doSetAsciiStream(bais, 100);
  }

  @Test
  public void testExecuteStringOnPreparedStatement() throws Exception {
    PreparedStatement pstmt = conn.prepareStatement("SELECT 1");

    try {
      pstmt.executeQuery("SELECT 2");
      fail("Expected an exception when executing a new SQL query on a prepared statement");
    }
    catch (SQLException e) {
      // Ok
    }

    try {
      pstmt.executeUpdate("UPDATE streamtable SET bin=bin");
      fail("Expected an exception when executing a new SQL update on a prepared statement");
    }
    catch (SQLException e) {
      // Ok
    }

    try {
      pstmt.execute("UPDATE streamtable SET bin=bin");
      fail("Expected an exception when executing a new SQL statement on a prepared statement");
    }
    catch (SQLException e) {
      // Ok
    }
  }

  @Test
  public void testBinaryStreamErrorsRestartable() throws SQLException {

    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    // InputStream is shorter than the length argument implies.
    InputStream is = new ByteArrayInputStream(buf);
    runBrokenStream(is, buf.length + 1);

    // InputStream throws an Exception during read.
    is = new BrokenInputStream(new ByteArrayInputStream(buf), buf.length / 2);
    runBrokenStream(is, buf.length);

    // Invalid length < 0.
    is = new ByteArrayInputStream(buf);
    runBrokenStream(is, -1);

    // Total Bind message length too long.
    is = new ByteArrayInputStream(buf);
    runBrokenStream(is, Integer.MAX_VALUE);
  }

  private void runBrokenStream(InputStream is, int length) throws SQLException {
    try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)")) {
      pstmt.setBinaryStream(1, is, length);
      pstmt.setString(2, "Other");
      pstmt.executeUpdate();
      fail("This isn't supposed to work.");
    }
    catch (SQLException sqle) {
      // don't need to rollback because we're in autocommit mode

      // verify the connection is still valid and the row didn't go in.
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM streamtable");
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
      rs.close();
      stmt.close();
    }
  }

  private void doSetBinaryStream(ByteArrayInputStream bais, int length) throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setBinaryStream(1, bais, length);
    pstmt.setString(2, null);
    pstmt.executeUpdate();
    pstmt.close();
  }

  private void doSetAsciiStream(InputStream is, int length) throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setBytes(1, null);
    pstmt.setAsciiStream(2, is, length);
    pstmt.executeUpdate();
    pstmt.close();
  }

  @Test
  public void testTrailingSpaces() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO texttable (ch, te, vc) VALUES (?, ?, ?) ");
    String str = "a  ";
    pstmt.setString(1, str);
    pstmt.setString(2, str);
    pstmt.setString(3, str);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("SELECT ch, te, vc FROM texttable WHERE ch=? AND te=? AND vc=?");
    pstmt.setString(1, str);
    pstmt.setString(2, str);
    pstmt.setString(3, str);
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(str, rs.getString(1));
    assertEquals(str, rs.getString(2));
    assertEquals(str, rs.getString(3));
    rs.close();
    pstmt.close();
  }

  @Test
  public void testSetNull() throws SQLException {
    // valid: fully qualified type to setNull()
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO texttable (te) VALUES (?)");
    pstmt.setNull(1, Types.VARCHAR);
    pstmt.executeUpdate();

    // valid: fully qualified type to setObject()
    pstmt.setObject(1, null, Types.VARCHAR);
    pstmt.executeUpdate();

    // setObject() with no type info
    pstmt.setObject(1, null);
    pstmt.executeUpdate();

    // setObject() with insufficient type info
    pstmt.setObject(1, null, Types.OTHER);
    pstmt.executeUpdate();

    // setNull() with insufficient type info
    pstmt.setNull(1, Types.OTHER);
    pstmt.executeUpdate();

    pstmt.close();
  }

  @Test
  public void testSingleQuotes() throws SQLException {
    String[] testStrings = new String[] {"bare ? question mark", "quoted \\' single quote", "doubled '' single quote", "octal \\060 constant", "escaped \\? question mark",
      "double \\\\ backslash", "double \" quote", };

    String[] testStringsStdConf = new String[] {"bare ? question mark", "quoted '' single quote", "doubled '' single quote", "octal 0 constant", "escaped ? question mark",
      "double \\ backslash", "double \" quote", };

    String[] expected = new String[] {"bare ? question mark", "quoted ' single quote", "doubled ' single quote", "octal 0 constant", "escaped ? question mark",
      "double \\ backslash", "double \" quote", };

    boolean oldStdStrings = TestUtil.getStandardConformingStrings(conn);
    Statement stmt = conn.createStatement();

    // Test with standard_conforming_strings turned off.
    stmt.execute("SET standard_conforming_strings TO off");
    for (int i = 0; i < testStrings.length; ++i) {
      PreparedStatement pstmt = conn.prepareStatement("SELECT '" + testStrings[i] + "'");
      ResultSet rs = pstmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(expected[i], rs.getString(1));
      rs.close();
      pstmt.close();
    }

    // Test with standard_conforming_strings turned off...
    // ... using the escape string syntax (E'').
    stmt.execute("SET standard_conforming_strings TO on");
    for (int i = 0; i < testStrings.length; ++i) {
      PreparedStatement pstmt = conn.prepareStatement("SELECT E'" + testStrings[i] + "'");
      ResultSet rs = pstmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(expected[i], rs.getString(1));
      rs.close();
      pstmt.close();
    }
    // ... using standard conforming input strings.
    for (int i = 0; i < testStrings.length; ++i) {
      PreparedStatement pstmt = conn.prepareStatement("SELECT '" + testStringsStdConf[i] + "'");
      ResultSet rs = pstmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(expected[i], rs.getString(1));
      rs.close();
      pstmt.close();
    }

    stmt.execute("SET standard_conforming_strings TO " + (oldStdStrings ? "on" : "off"));
    stmt.close();
  }

  @Test
  public void testDoubleQuotes() throws SQLException {
    String[] testStrings = new String[] {"bare ? question mark", "single ' quote", "doubled '' single quote", "doubled \"\" double quote", "no backslash interpretation here: \\", };

    for (int i = 0; i < testStrings.length; ++i) {
      PreparedStatement pstmt = conn.prepareStatement("CREATE TABLE \"" + testStrings[i] + "\" (i integer)");
      pstmt.executeUpdate();
      pstmt.close();

      pstmt = conn.prepareStatement("DROP TABLE \"" + testStrings[i] + "\"");
      pstmt.executeUpdate();
      pstmt.close();
    }
  }

  @Test
  public void testDollarQuotes() throws SQLException {

    PreparedStatement st;
    ResultSet rs;

    st = conn.prepareStatement("SELECT $$;$$ WHERE $x$?$x$=$_0$?$_0$ AND $$?$$=?");
    st.setString(1, "?");
    rs = st.executeQuery();
    assertTrue(rs.next());
    assertEquals(";", rs.getString(1));
    assertFalse(rs.next());
    st.close();

    st = conn.prepareStatement("SELECT $__$;$__$ WHERE ''''=$q_1$'$q_1$ AND ';'=?;");
    st.setString(1, ";");
    assertTrue(st.execute());
    rs = st.getResultSet();
    assertTrue(rs.next());
    assertEquals(";", rs.getString(1));
    assertFalse(rs.next());

    st = conn.prepareStatement("SELECT $x$$a$;$x $a$$x$ WHERE $$;$$=? OR ''=$c$c$;$c$;");
    st.setString(1, ";");
    assertTrue(st.execute());
    rs = st.getResultSet();
    assertTrue(rs.next());
    assertEquals("$a$;$x $a$", rs.getString(1));
    assertFalse(rs.next());

    st = conn.prepareStatement("SELECT ?::text");
    st.setString(1, "$a$ $a$");
    assertTrue(st.execute());
    rs = st.getResultSet();
    assertTrue(rs.next());
    assertEquals("$a$ $a$", rs.getString(1));

    assertFalse(rs.next());

    st.close();
  }

  @Test
  public void testDollarQuotesAndIdentifiers() throws SQLException {

    PreparedStatement st;

    conn.createStatement().execute("CREATE TEMP TABLE a$b$c(a varchar, b varchar)");
    st = conn.prepareStatement("INSERT INTO a$b$c (a, b) VALUES (?, ?)");
    st.setString(1, "a");
    st.setString(2, "b");
    st.executeUpdate();
    st.close();

    conn.createStatement().execute("CREATE TEMP TABLE e$f$g(h varchar, e$f$g varchar) ");
    st = conn.prepareStatement("UPDATE e$f$g SET h = ? || e$f$g");
    st.setString(1, "a");
    st.executeUpdate();
    st.close();
  }

  @Test
  public void testComments() throws SQLException {
    Statement st;
    PreparedStatement pst;
    ResultSet rs;

    st = conn.createStatement();
    assertTrue(st.execute("SELECT /*?*/ /*/*/*/**/*/*/*/1;SELECT 1;--SELECT 1"));
    assertTrue(st.getMoreResults());
    assertFalse(st.getMoreResults());
    st.close();

    pst = conn.prepareStatement("SELECT /**/'?'/*/**/*/ WHERE '?'=/*/*/*?*/*/*/--?\n?");
    pst.setString(1, "?");
    rs = pst.executeQuery();
    assertTrue(rs.next());
    assertEquals("?", rs.getString(1));
    assertFalse(rs.next());
    pst.close();
  }

  @Test
  public void testDouble() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE double_tab (max_double float, min_double float, null_value float)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into double_tab values (?,?,?)");
    pstmt.setDouble(1, 1.0E125);
    pstmt.setDouble(2, 1.0E-130);
    pstmt.setNull(3, Types.DOUBLE);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from double_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    rs.getDouble(1);
    assertTrue(rs.getDouble(1) == 1.0E125);
    assertTrue(rs.getDouble(2) == 1.0E-130);
    rs.getDouble(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testFloat() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE float_tab (max_float real, min_float real, null_value real)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into float_tab values (?,?,?)");
    pstmt.setFloat(1, (float) 1.0E37);
    pstmt.setFloat(2, (float) 1.0E-37);
    pstmt.setNull(3, Types.FLOAT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from float_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    rs.getFloat(1);
    assertTrue("expected 1.0E37,received " + rs.getFloat(1), rs.getFloat(1) == (float) 1.0E37);
    assertTrue("expected 1.0E-37,received " + rs.getFloat(2), rs.getFloat(2) == (float) 1.0E-37);
    rs.getDouble(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testBoolean() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE bool_tab (max_val boolean, min_val boolean, null_val boolean)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into bool_tab values (?,?,?)");
    pstmt.setBoolean(1, true);
    pstmt.setBoolean(2, false);
    pstmt.setNull(3, Types.BIT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from bool_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected true,received " + rs.getBoolean(1), rs.getBoolean(1) == true);
    assertTrue("expected false,received " + rs.getBoolean(2), rs.getBoolean(2) == false);
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetFloatInteger() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE float_tab (max_val float8, min_val float, null_val float8)");
    pstmt.executeUpdate();
    pstmt.close();

    Integer maxInteger = new Integer(2147483647), minInteger = new Integer(-2147483648);

    Double maxFloat = new Double(2147483647), minFloat = new Double(-2147483648);

    pstmt = conn.prepareStatement("insert into float_tab values (?,?,?)");
    pstmt.setObject(1, maxInteger, Types.FLOAT);
    pstmt.setObject(2, minInteger, Types.FLOAT);
    pstmt.setNull(3, Types.FLOAT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from float_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + maxFloat + " ,received " + rs.getObject(1), ((Double) rs.getObject(1)).equals(maxFloat));
    assertTrue("expected " + minFloat + " ,received " + rs.getObject(2), ((Double) rs.getObject(2)).equals(minFloat));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetFloatString() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE float_tab (max_val float8, min_val float8, null_val float8)");
    pstmt.executeUpdate();
    pstmt.close();

    String maxStringFloat = new String("1.0E37"), minStringFloat = new String("1.0E-37");
    Double maxFloat = new Double(1.0E37), minFloat = new Double(1.0E-37);

    pstmt = conn.prepareStatement("insert into float_tab values (?,?,?)");
    pstmt.setObject(1, maxStringFloat, Types.FLOAT);
    pstmt.setObject(2, minStringFloat, Types.FLOAT);
    pstmt.setNull(3, Types.FLOAT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from float_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected true,received " + rs.getObject(1), ((Double) rs.getObject(1)).equals(maxFloat));
    assertTrue("expected false,received " + rs.getBoolean(2), ((Double) rs.getObject(2)).equals(minFloat));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetFloatBigDecimal() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE float_tab (max_val float8, min_val float8, null_val float8)");
    pstmt.executeUpdate();
    pstmt.close();

    BigDecimal maxBigDecimalFloat = new BigDecimal("1.0E37"), minBigDecimalFloat = new BigDecimal("1.0E-37");
    Double maxFloat = new Double(1.0E37), minFloat = new Double(1.0E-37);

    pstmt = conn.prepareStatement("insert into float_tab values (?,?,?)");
    pstmt.setObject(1, maxBigDecimalFloat, Types.FLOAT);
    pstmt.setObject(2, minBigDecimalFloat, Types.FLOAT);
    pstmt.setNull(3, Types.FLOAT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from float_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + maxFloat + " ,received " + rs.getObject(1), ((Double) rs.getObject(1)).equals(maxFloat));
    assertTrue("expected " + minFloat + " ,received " + rs.getObject(2), ((Double) rs.getObject(2)).equals(minFloat));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetTinyIntFloat() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE tiny_int (max_val int4, min_val int4, null_val int4)");
    pstmt.executeUpdate();
    pstmt.close();

    Integer maxInt = new Integer(127), minInt = new Integer(-127);
    Float maxIntFloat = new Float(127), minIntFloat = new Float(-127);

    pstmt = conn.prepareStatement("insert into tiny_int values (?,?,?)");
    pstmt.setObject(1, maxIntFloat, Types.TINYINT);
    pstmt.setObject(2, minIntFloat, Types.TINYINT);
    pstmt.setNull(3, Types.TINYINT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from tiny_int");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + maxInt + " ,received " + rs.getObject(1), ((Integer) rs.getObject(1)).equals(maxInt));
    assertTrue("expected " + minInt + " ,received " + rs.getObject(2), ((Integer) rs.getObject(2)).equals(minInt));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetSmallIntFloat() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE small_int (max_val int4, min_val int4, null_val int4)");
    pstmt.executeUpdate();
    pstmt.close();

    Integer maxInt = new Integer(32767), minInt = new Integer(-32768);
    Float maxIntFloat = new Float(32767), minIntFloat = new Float(-32768);

    pstmt = conn.prepareStatement("insert into small_int values (?,?,?)");
    pstmt.setObject(1, maxIntFloat, Types.SMALLINT);
    pstmt.setObject(2, minIntFloat, Types.SMALLINT);
    pstmt.setNull(3, Types.TINYINT);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from small_int");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + maxInt + " ,received " + rs.getObject(1), ((Integer) rs.getObject(1)).equals(maxInt));
    assertTrue("expected " + minInt + " ,received " + rs.getObject(2), ((Integer) rs.getObject(2)).equals(minInt));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetIntFloat() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE int_TAB (max_val int4, min_val int4, null_val int4)");
    pstmt.executeUpdate();
    pstmt.close();

    Integer maxInt = new Integer(1000), minInt = new Integer(-1000);
    Float maxIntFloat = new Float(1000), minIntFloat = new Float(-1000);

    pstmt = conn.prepareStatement("insert into int_tab values (?,?,?)");
    pstmt.setObject(1, maxIntFloat, Types.INTEGER);
    pstmt.setObject(2, minIntFloat, Types.INTEGER);
    pstmt.setNull(3, Types.INTEGER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from int_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + maxInt + " ,received " + rs.getObject(1), ((Integer) rs.getObject(1)).equals(maxInt));
    assertTrue("expected " + minInt + " ,received " + rs.getObject(2), ((Integer) rs.getObject(2)).equals(minInt));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetBooleanDouble() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE double_tab (max_val float, min_val float, null_val float)");
    pstmt.executeUpdate();
    pstmt.close();

    Boolean trueVal = Boolean.TRUE, falseVal = Boolean.FALSE;
    Double dBooleanTrue = new Double(1), dBooleanFalse = new Double(0);

    pstmt = conn.prepareStatement("insert into double_tab values (?,?,?)");
    pstmt.setObject(1, trueVal, Types.DOUBLE);
    pstmt.setObject(2, falseVal, Types.DOUBLE);
    pstmt.setNull(3, Types.DOUBLE);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from double_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1), ((Double) rs.getObject(1)).equals(dBooleanTrue));
    assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2), ((Double) rs.getObject(2)).equals(dBooleanFalse));
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetBooleanNumeric() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE numeric_tab (max_val numeric(30,15), min_val numeric(30,15), null_val numeric(30,15))");
    pstmt.executeUpdate();
    pstmt.close();

    Boolean trueVal = Boolean.TRUE, falseVal = Boolean.FALSE;
    BigDecimal dBooleanTrue = new BigDecimal(1), dBooleanFalse = new BigDecimal(0);

    pstmt = conn.prepareStatement("insert into numeric_tab values (?,?,?)");
    pstmt.setObject(1, trueVal, Types.NUMERIC, 2);
    pstmt.setObject(2, falseVal, Types.NUMERIC, 2);
    pstmt.setNull(3, Types.DOUBLE);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from numeric_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1), ((BigDecimal) rs.getObject(1)).compareTo(dBooleanTrue) == 0);
    assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2), ((BigDecimal) rs.getObject(2)).compareTo(dBooleanFalse) == 0);
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testSetBooleanDecimal() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE temp TABLE DECIMAL_TAB (max_val numeric(30,15), min_val numeric(30,15), null_val numeric(30,15))");
    pstmt.executeUpdate();
    pstmt.close();

    Boolean trueVal = Boolean.TRUE, falseVal = Boolean.FALSE;
    BigDecimal dBooleanTrue = new BigDecimal(1), dBooleanFalse = new BigDecimal(0);

    pstmt = conn.prepareStatement("insert into DECIMAL_TAB values (?,?,?)");
    pstmt.setObject(1, trueVal, Types.DECIMAL, 2);
    pstmt.setObject(2, falseVal, Types.DECIMAL, 2);
    pstmt.setNull(3, Types.DOUBLE);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from DECIMAL_TAB");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());

    assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1), ((BigDecimal) rs.getObject(1)).compareTo(dBooleanTrue) == 0);
    assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2), ((BigDecimal) rs.getObject(2)).compareTo(dBooleanFalse) == 0);
    rs.getFloat(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();

  }

  @Test
  public void testRowId() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO texttable (te) VALUES (?)", new String[] {"ctid"});
    pstmt.setString(1, "some text");
    pstmt.executeUpdate();
    ResultSet keys = pstmt.getGeneratedKeys();
    assertTrue(keys.next());
    RowId rowId = keys.getRowId(1);
    keys.close();
    pstmt.close();

    pstmt = conn.prepareStatement("SELECT te FROM texttable WHERE ctid = ?");
    pstmt.setRowId(1, rowId);
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("some text", rs.getString(1));
    rs.close();
    pstmt.close();
  }

  @Test
  public void testSetObjectBinary() throws SQLException {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setObject(1, buf, Types.BINARY);
    pstmt.setString(2, null);
    pstmt.executeUpdate();
    pstmt.close();

    ((PGConnectionImpl)conn).setStrictMode(true);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bin FROM streamtable");
    assertTrue(rs.next());
    assertTrue(Arrays.equals(buf, (byte[])rs.getObject(1)));
    rs.close();
    stmt.close();
    ((PGConnectionImpl)conn).setStrictMode(false);
  }

  @Test
  public void testSetObjectVarBinary() throws SQLException {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setObject(1, buf, Types.VARBINARY);
    pstmt.setString(2, null);
    pstmt.executeUpdate();
    pstmt.close();

    ((PGConnectionImpl)conn).setStrictMode(true);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bin FROM streamtable");
    assertTrue(rs.next());
    assertTrue(Arrays.equals(buf, (byte[])rs.getObject(1)));
    rs.close();
    stmt.close();
    ((PGConnectionImpl)conn).setStrictMode(false);
  }

  @Test
  public void testSetObjectLongVarBinary() throws SQLException {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setObject(1, buf, Types.LONGVARBINARY);
    pstmt.setString(2, null);
    pstmt.executeUpdate();
    pstmt.close();

    ((PGConnectionImpl)conn).setStrictMode(true);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bin FROM streamtable");
    assertTrue(rs.next());
    assertTrue(Arrays.equals(buf, (byte[])rs.getObject(1)));
    rs.close();
    stmt.close();
    ((PGConnectionImpl)conn).setStrictMode(false);
  }

  @Test
  public void testClearParameters() throws SQLException {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    pstmt.setObject(1, buf, Types.BINARY);
    pstmt.setString(2, null);
    pstmt.clearParameters();
    try {
      pstmt.executeQuery();
      fail("Failed");
    }
    catch (SQLException se) {
      // Correct
    }
    finally {
      pstmt.close();
    }
  }

  @Test
  public void testExecuteWithoutParameters() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    try {
      pstmt.execute();
      fail("Failed");
    }
    catch (SQLException se) {
      // Correct
    }
    finally {
      pstmt.close();
    }
  }

  @Test
  public void testExecuteQueryWithoutParameters() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    try {
      pstmt.executeQuery();
      fail("Failed");
    }
    catch (SQLException se) {
      // Correct
    }
    finally {
      pstmt.close();
    }
  }

  @Test
  public void testExecuteUpdateWithoutParameters() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streamtable (bin,str) VALUES (?,?)");
    try {
      pstmt.executeUpdate();
      fail("Failed");
    }
    catch (SQLException se) {
      // Correct
    }
    finally {
      pstmt.close();
    }
  }

  @Test
  public void testInet() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE inet_tab (ip1 inet, ip2 inet, ip3 inet)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into inet_tab values (?,?,?)");
    InetAddr inet1;
    InetAddr inet2;
    pstmt.setObject(1, inet1 = new InetAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1"));
    pstmt.setObject(2, inet2 = new InetAddr("192.168.100.128/25"));
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from inet_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == InetAddr.class);
    assertTrue(inet1.equals(rs.getObject(1)));
    assertTrue(rs.getObject(2).getClass() == InetAddr.class);
    assertTrue(inet2.equals(rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testCidr() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE cidr_tab (ip1 cidr, ip2 cidr, ip3 cidr)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into cidr_tab values (?,?,?)");
    CidrAddr cidr1;
    CidrAddr cidr2;
    pstmt.setObject(1, cidr1 = new CidrAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1"));
    pstmt.setObject(2, cidr2 = new CidrAddr("2001:4f8:3:ba::/64"));
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from cidr_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == CidrAddr.class);
    assertTrue(cidr1.equals(rs.getObject(1)));
    assertTrue(rs.getObject(2).getClass() == CidrAddr.class);
    assertTrue(cidr2.equals(rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testPoint() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE point_tab (p1 point, p2 point, p3 point)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into point_tab values (?,?,?)");
    double[] p1 = new double[] {45.0, 56.3};
    double[] p2 = new double[] {0, 0};
    pstmt.setObject(1, p1);
    pstmt.setObject(2, p2);
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from point_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == double[].class);
    assertTrue(Arrays.equals(p1, (double[]) rs.getObject(1)));
    assertTrue(Arrays.equals(p2, (double[]) rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testPath() throws SQLException {
    try (PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE path_tab (p1 path, p2 path, p3 path)")) {
      pstmt.executeUpdate();
    }
    Path p1 = GeometryParsers.INSTANCE.parsePath("[(678.6,454),(10,89),(124.6,0)]");
    Path p2 = GeometryParsers.INSTANCE.parsePath("((678.6,454),(10,89),(124.6,0))");
    try (PreparedStatement pstmt = conn.prepareStatement("insert into path_tab values (?,?,?)")) {
      pstmt.setObject(1, p1);
      pstmt.setObject(2, p2);
      pstmt.setObject(3, null, Types.OTHER);
      pstmt.executeUpdate();
    }

    try (PreparedStatement pstmt = conn.prepareStatement("select * from path_tab");
        ResultSet rs = pstmt.executeQuery()) {
      assertTrue(rs.next());
      assertTrue(rs.getObject(1).getClass() == Path.class);
      assertTrue(p1.equals(rs.getObject(1)));
      assertTrue(p2.equals(rs.getObject(2)));
      rs.getObject(3);
      assertTrue(rs.wasNull());
    }
  }

  @Test
  public void testPolygon() throws SQLException {
    try (PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE polygon_tab (p1 polygon, p2 polygon, p3 polygon)")) {
      pstmt.executeUpdate();
    }
    double[][] p1 = GeometryParsers.INSTANCE.parsePolygon("((678.6,454),(10,89),(124.6,0),(0,0))");
    double[][] p2 = GeometryParsers.INSTANCE.parsePolygon("((678.6,454),(10,89),(124.6,0))");
    try (PreparedStatement pstmt = conn.prepareStatement("insert into polygon_tab values (?,?,?)")) {
      pstmt.setObject(1, p1);
      pstmt.setObject(2, p2);
      pstmt.setObject(3, null, Types.OTHER);
      pstmt.executeUpdate();
    }

    try (PreparedStatement pstmt = conn.prepareStatement("select * from polygon_tab");
        ResultSet rs = pstmt.executeQuery()) {
      assertTrue(rs.next());
      assertTrue(rs.getObject(1).getClass() == double[][].class);
      assertTrue(Arrays.deepEquals(p1, (double[][]) rs.getObject(1)));
      assertTrue(Arrays.deepEquals(p2, (double[][]) rs.getObject(2)));
      rs.getObject(3);
      assertTrue(rs.wasNull());
    }
  }

  @Test
  public void testCircle() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE circle_tab (p1 circle, p2 circle, p3 circle)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into circle_tab values (?,?,?)");
    double[] p1 = new double[] {45.0, 56.3, 40};
    double[] p2 = new double[] {0, 0, 0};
    pstmt.setObject(1, p1);
    pstmt.setObject(2, p2);
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from circle_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == double[].class);
    assertTrue(Arrays.equals(p1, (double[]) rs.getObject(1)));
    assertTrue(Arrays.equals(p2, (double[]) rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testLSeg() throws SQLException {
    testLSeg("lseg");
  }

  @Test
  public void testBox() throws SQLException {
    testLSeg("box");
  }

  private void testLSeg(String pgtype) throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE " + pgtype + "_tab (p1 " + pgtype + ", p2 " + pgtype + ", p3 " + pgtype + ")");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into " + pgtype + "_tab values (?,?,?)");
    double[] p1 = new double[] {45.0, 60.0, 40.9, 56.3};
    double[] p2 = new double[] {0, 0, 0, 0};
    pstmt.setObject(1, p1);
    pstmt.setObject(2, p2);
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from " + pgtype + "_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == double[].class);
    assertTrue(Arrays.equals(p1, (double[]) rs.getObject(1)));
    assertTrue(Arrays.equals(p2, (double[]) rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testMacAddr() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE mac_tab (mac1 macaddr, mac2 macaddr, mac3 macaddr)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into mac_tab values (?,?,?)");
    byte[] mac1 = new byte[] {0x08, 0x00, 0x2b, 0x01, 0x02, 0x03};
    byte[] mac2 = new byte[] {0x08, 0x4f, 0x2a, 0x01, 0x02, 0x3e};
    pstmt.setObject(1, mac1);
    pstmt.setObject(2, mac2);
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from mac_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getObject(1).getClass() == byte[].class);
    assertTrue(Arrays.equals(mac1, (byte[]) rs.getObject(1)));
    assertTrue(Arrays.equals(mac2, (byte[]) rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testHStore() throws SQLException {

    if (!TestUtil.isExtensionInstalled(conn, "hstore")) {
      System.out.println("Skipping hstore (extension not intalled)");
      return;
    }

    PreparedStatement pstmt = conn.prepareStatement("CREATE TEMP TABLE hstore_tab (hs1 hstore, hs2 hstore, hs3 hstore)");
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("insert into hstore_tab values (?,?,?)");
    Map<String, String> hs1 = new HashMap<>();
    hs1.put("k1", "v1");
    hs1.put("k2", "v2");
    hs1.put("k3", "v3");
    hs1.put("k4", "v4");
    Map<String, String> hs2 = new HashMap<>();
    pstmt.setObject(1, hs1);
    pstmt.setObject(2, hs2);
    pstmt.setObject(3, null, Types.OTHER);
    pstmt.executeUpdate();
    pstmt.close();

    pstmt = conn.prepareStatement("select * from hstore_tab");
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(HashMap.class.equals(rs.getObject(1).getClass()));
    assertTrue(hs1.equals(rs.getObject(1)));
    assertTrue(hs2.equals(rs.getObject(2)));
    rs.getObject(3);
    assertTrue(rs.wasNull());
    rs.close();
    pstmt.close();
  }

//TODO: reconcile against mainstream driver
//  public void testUnknownSetObject() throws SQLException {
//    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO intervaltable(i) VALUES (?)");
//
//    pstmt.setString(1, "1 week");
//    try {
//      pstmt.executeUpdate();
//      fail("Should have failed with type mismatch.");
//    }
//    catch (SQLException sqle) {
//    }
//
//    pstmt.setObject(1, "1 week", Types.OTHER);
//    pstmt.executeUpdate();
//    pstmt.close();
//  }

  /**
   * With autoboxing this apparently happens more often now.
   */
  @Test
  public void testSetObjectCharacter() throws SQLException {
    PreparedStatement ps = conn.prepareStatement("INSERT INTO texttable(te) VALUES (?)");
    ps.setObject(1, new Character('z'));
    ps.executeUpdate();
    ps.close();
  }

  /**
   * When we have parameters of unknown type and it's not using the unnamed
   * statement, we issue a protocol level statment describe message for the V3
   * protocol. This test just makes sure that works.
   */
  @Test
  public void testStatementDescribe() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("SELECT ?::int");
    pstmt.setObject(1, new Integer(2));
    for (int i = 0; i < 10; i++) {
      ResultSet rs = pstmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs.close();
    }
    pstmt.close();
  }

  @Test
  public void testChangeType() throws SQLException {
    Connection c = conn;
    List<String> statements = Arrays.asList(
        "insert into inttable(a) values(1)",
        "select * from inttable",
        "alter table inttable add b int",
        "select * from inttable",
        "alter table inttable add c int",
        "select * from inttable",
        "alter table inttable add d int",
        "select * from inttable",
        "alter table inttable add e int",
        "select * from inttable",
        "alter table inttable add f int",
        "select * from inttable"
    );

    for (String statement : statements) {
      PreparedStatement s = null;
      try {
        s = c.prepareStatement(statement);
        if (!statement.startsWith("select")) {
          s.execute();
          continue;
        }
        ResultSet rs = s.executeQuery();
        assertInttTableSanity(rs);
        rs.close();
      } finally {
        if (s != null) {
          s.close();
        }
      }
    }
  }

  private void assertInttTableSanity(ResultSet rs) throws SQLException {
    assertEquals("inttable should have one row", true, rs.next());
    assertEquals("inttable.a should be 1", 1, rs.getInt(1));
    assertEquals("inttable should have exactly one row", false, rs.next());
  }

  @Test
  public void testDeallocateAll() throws SQLException {
    Statement s = conn.createStatement();
    s.execute("insert into inttable(a) values(1)");

    PreparedStatement ps = conn.prepareStatement("select * from inttable");

    for (int i = 0; i < 10; i++) {
      ps.execute();
    }

    s.execute("DEALLOCATE ALL");
    s.close();

    ResultSet rs = ps.executeQuery();
    assertInttTableSanity(rs);
    rs.close();
  }

  /**
   * No cache
   */
  @Test
  public void testNoCache() throws SQLException {
    PGDataSource ds = new PGDataSource();
    ds.setHost(TestUtil.getServer());
    ds.setPort(Integer.valueOf(TestUtil.getPort()));
    ds.setDatabase(TestUtil.getDatabase());
    ds.setUser(TestUtil.getUser());
    ds.setPassword(TestUtil.getPassword());
    ds.setPreparedStatementCacheSize(0);

    Connection c = null;
    PreparedStatement pstmt = null;
    try {
      c = ds.getConnection();
      pstmt = c.prepareStatement("SELECT ?::int");
      for (int i = 0; i < 2; i++) {
        pstmt.setObject(1, new Integer(i));
        ResultSet rs = pstmt.executeQuery();
        rs.close();
      }
    }
    finally {
      try {
        if (pstmt != null)
          pstmt.close();
      }
      catch (SQLException ignore) {
        // Ignore
      }
      try {
        if (c != null)
          c.close();
      }
      catch (SQLException ignore) {
        // Ignore
      }
    }
  }

}
