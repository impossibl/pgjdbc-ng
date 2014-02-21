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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ResultSetMetaDataTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "rsmd1", "a int primary key, b text, c decimal(10,2)", true);
    TestUtil.createTable(conn, "timetest", "tm time(3), tmtz timetz, ts timestamp without time zone, tstz timestamp(6) with time zone");

    TestUtil.dropSequence(conn, "serialtest_a_seq");
    TestUtil.dropSequence(conn, "serialtest_b_seq");
    TestUtil.createTable(conn, "serialtest", "a serial, b bigserial, c int");
    TestUtil
        .createTable(
            conn,
            "alltypes",
            "bool boolean, i2 int2, i4 int4, i8 int8, num numeric(10,2), re real, fl float, ch char(3), vc varchar(3), tx text, d date, t time without time zone, tz time with time zone, ts timestamp without time zone, tsz timestamp with time zone, bt bytea");
    TestUtil.createTable(conn, "sizetest",
        "fixedchar char(5), fixedvarchar varchar(5), unfixedvarchar varchar, txt text, bytearr bytea, num64 numeric(6,4), num60 numeric(6,0), num numeric, ip inet");
    TestUtil.createTable(conn, "compositetest", "col rsmd1");
  }

  @After
  public void after() throws Exception {
    TestUtil.dropTable(conn, "compositetest");
    TestUtil.dropTable(conn, "rsmd1");
    TestUtil.dropTable(conn, "timetest");
    TestUtil.dropTable(conn, "serialtest");
    TestUtil.dropTable(conn, "alltypes");
    TestUtil.dropTable(conn, "sizetest");
    TestUtil.dropSequence(conn, "serialtest_a_seq");
    TestUtil.dropSequence(conn, "serialtest_b_seq");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testStandardResultSet() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT a,b,c,a+c as total,oid,b as d FROM rsmd1");
    runStandardTests(rs.getMetaData());
    rs.close();
    stmt.close();
  }

  @Test
  public void testPreparedResultSet() throws SQLException {

    PreparedStatement pstmt = conn.prepareStatement("SELECT a,b,c,a+c as total,oid,b as d FROM rsmd1 WHERE b = ?");
    runStandardTests(pstmt.getMetaData());
    pstmt.close();
  }

  private void runStandardTests(ResultSetMetaData rsmd) throws SQLException {

    assertEquals(6, rsmd.getColumnCount());

    assertEquals("a", rsmd.getColumnLabel(1));
    assertEquals("total", rsmd.getColumnLabel(4));

    assertEquals("a", rsmd.getColumnName(1));
    assertEquals("oid", rsmd.getColumnName(5));
    assertEquals("total", rsmd.getColumnName(4));
    assertEquals("b", rsmd.getColumnName(6));

    assertEquals(Types.INTEGER, rsmd.getColumnType(1));
    assertEquals(Types.VARCHAR, rsmd.getColumnType(2));

    assertEquals("int4", rsmd.getColumnTypeName(1));
    assertEquals("text", rsmd.getColumnTypeName(2));

    assertEquals(10, rsmd.getPrecision(3));

    assertEquals(2, rsmd.getScale(3));

    assertEquals("public", rsmd.getSchemaName(1));
    assertEquals("", rsmd.getSchemaName(4));

    assertEquals("rsmd1", rsmd.getTableName(1));
    assertEquals("", rsmd.getTableName(4));

    assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
    assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
    assertEquals(ResultSetMetaData.columnNullableUnknown, rsmd.isNullable(4));
  }

  // verify that a prepared update statement returns no metadata and doesn't
  // execute.
  @Test
  public void testPreparedUpdate() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO rsmd1(a,b) VALUES(?,?)");
    pstmt.setInt(1, 1);
    pstmt.setString(2, "hello");
    ResultSetMetaData rsmd = pstmt.getMetaData();
    assertEquals(0, rsmd.getColumnCount());
    pstmt.close();

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM rsmd1");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
    rs.close();
    stmt.close();
  }

  @Test
  public void testDatabaseMetaDataNames() throws SQLException {
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    ResultSet resultSet = databaseMetaData.getTableTypes();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(1, resultSetMetaData.getColumnCount());
    assertEquals("TABLE_TYPE", resultSetMetaData.getColumnName(1));
    resultSet.close();
  }

  @Test
  public void testTimestampInfo() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT tm, tmtz, ts, tstz FROM timetest");
    ResultSetMetaData rsmd = rs.getMetaData();

    // For reference:
    // TestUtil.createTable(conn, "timetest",
    // "tm time(3), tmtz timetz, ts timestamp without time zone, tstz timestamp(6) with time zone");

    assertEquals(3, rsmd.getScale(1));
    assertEquals(6, rsmd.getScale(2));
    assertEquals(6, rsmd.getScale(3));
    assertEquals(6, rsmd.getScale(4));

    assertEquals(12, rsmd.getColumnDisplaySize(1));
    assertEquals(21, rsmd.getColumnDisplaySize(2));
    assertEquals(29, rsmd.getColumnDisplaySize(3));
    assertEquals(35, rsmd.getColumnDisplaySize(4));

    rs.close();
    stmt.close();
  }

  @Test
  public void testColumnDisplaySize() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT fixedchar, fixedvarchar, unfixedvarchar, txt, bytearr, num64, num60, num, ip FROM sizetest");
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(5, rsmd.getColumnDisplaySize(1));
    assertEquals(5, rsmd.getColumnDisplaySize(2));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(3));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(4));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(5));
    assertEquals(8, rsmd.getColumnDisplaySize(6));
    assertEquals(7, rsmd.getColumnDisplaySize(7));
    assertEquals(131089, rsmd.getColumnDisplaySize(8));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(9));
  }

  @Test
  public void testIsAutoIncrement() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT c,b,a FROM serialtest");
    ResultSetMetaData rsmd = rs.getMetaData();

    assertTrue(!rsmd.isAutoIncrement(1));
    assertTrue(rsmd.isAutoIncrement(2));
    assertTrue(rsmd.isAutoIncrement(3));
    assertEquals("bigserial", rsmd.getColumnTypeName(2));
    assertEquals("serial", rsmd.getColumnTypeName(3));

    rs.close();
    stmt.close();
  }

  @Test
  public void testClassesMatch() throws SQLException, ClassNotFoundException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("INSERT INTO alltypes (bool, i2, i4, i8, num, re, fl, ch, vc, tx, d, t, tz, ts, tsz, bt) VALUES ('t', 2, 4, 8, 3.1, 3.14, 3.141, 'c', 'vc', 'tx', '2004-04-09', '09:01:00', '11:11:00-01','2004-04-09 09:01:00','1999-09-19 14:23:12-09', '\\\\123')");
    ResultSet rs = stmt.executeQuery("SELECT * FROM alltypes");
    ResultSetMetaData rsmd = rs.getMetaData();
    assertTrue(rs.next());
    for (int i = 0; i < rsmd.getColumnCount(); i++) {
      Class<?> cls = Class.forName(rsmd.getColumnClassName(i + 1));
      assertTrue(cls.isAssignableFrom(rs.getObject(i + 1).getClass()));
    }
  }

  @Test
  public void testComposite() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT col FROM compositetest");
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals(Types.STRUCT, rsmd.getColumnType(1));
    assertEquals("rsmd1", rsmd.getColumnTypeName(1));
  }

}
