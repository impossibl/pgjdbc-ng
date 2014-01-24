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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ArrayTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "arrtest", "intarr int[], decarr decimal(2,1)[], strarr text[], str text");
  }

  @After
  public void after() throws SQLException {
    TestUtil.dropTable(conn, "arrtest");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testSetNull() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest VALUES (?,?,?)");
    pstmt.setNull(1, Types.ARRAY);
    pstmt.setNull(2, Types.ARRAY);
    pstmt.setNull(3, Types.ARRAY);
    pstmt.executeUpdate();

    pstmt.setObject(1, null, Types.ARRAY);
    pstmt.setObject(2, null);
    pstmt.setObject(3, null);
    pstmt.executeUpdate();

    pstmt.setArray(1, null);
    pstmt.setArray(2, null);
    pstmt.setArray(3, null);
    pstmt.executeUpdate();

    pstmt.close();
  }

  @Test
  public void testGetNull() throws SQLException {
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("SELECT null::int[]");
    assertTrue(rs.next());
    assertEquals(null, rs.getArray(1));
    assertEquals(null, rs.getObject(1));
    assertArrayEquals(null, rs.getObject(1, Integer[].class));

    rs.close();
    stmt.close();
  }

  @Test
  public void testSendRecvMultiple() throws SQLException {

    PreparedStatement ps = conn.prepareStatement("SELECT ?::int[], ?::decimal(2,1)[], ?::text[]");
    ps.setObject(1, new Integer[]{1, 2, 3});
    ps.setObject(2, new Float[]{3.1f, 1.4f});
    ps.setObject(3, new String[] {"abc", "def"});

    ResultSet rs = ps.executeQuery();
    assertTrue(rs.next());

    Array arr = rs.getArray(1);
    assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());

    arr = rs.getArray(2);
    assertEquals(Types.NUMERIC, arr.getBaseType());
    BigDecimal[] decarr = (BigDecimal[]) arr.getArray();
    assertEquals(2, decarr.length);
    assertEquals(new BigDecimal("3.1"), decarr[0]);
    assertEquals(new BigDecimal("1.4"), decarr[1]);

    arr = rs.getArray(3);
    assertEquals(Types.VARCHAR, arr.getBaseType());
    String[] strarr = (String[]) arr.getArray();
    assertEquals("abc", strarr[0]);
    assertEquals("def", strarr[1]);
    ps.close();
  }

  @Test
  public void testRetrieveArrays() throws SQLException {
    Statement stmt = conn.createStatement();

    // you need a lot of backslashes to get a double quote in.
    stmt.executeUpdate("INSERT INTO arrtest VALUES ('{1,2,3}','{3.1,1.4}', '{abc,f''a,fa\\\"b,def}')");

    ResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest");
    assertTrue(rs.next());

    Array arr = rs.getArray(1);
    assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());

    arr = rs.getArray(2);
    assertEquals(Types.NUMERIC, arr.getBaseType());
    BigDecimal[] decarr = (BigDecimal[]) arr.getArray();
    assertEquals(2, decarr.length);
    assertEquals(new BigDecimal("3.1"), decarr[0]);
    assertEquals(new BigDecimal("1.4"), decarr[1]);

    arr = rs.getArray(3);
    assertEquals(Types.VARCHAR, arr.getBaseType());
    String[] strarr = (String[]) arr.getArray(2, 2);
    assertEquals(2, strarr.length);
    assertEquals("f'a", strarr[0]);
    assertEquals("fa\"b", strarr[1]);

    rs.close();
    stmt.close();
  }

  @Test
  public void testRetrieveResultSets() throws SQLException {
    Statement stmt = conn.createStatement();

    // you need a lot of backslashes to get a double quote in.
    stmt.executeUpdate("INSERT INTO arrtest VALUES ('{1,2,3}','{3.1,1.4}', '{abc,f''a,fa\\\"b,def}')");

    ResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest");
    assertTrue(rs.next());

    Array arr = rs.getArray(1);
    assertEquals(Types.INTEGER, arr.getBaseType());
    ResultSet arrrs = arr.getResultSet();
    assertTrue(arrrs.next());
    assertEquals(1, arrrs.getInt(1));
    assertEquals(1, arrrs.getInt(2));
    assertTrue(arrrs.next());
    assertEquals(2, arrrs.getInt(1));
    assertEquals(2, arrrs.getInt(2));
    assertTrue(arrrs.next());
    assertEquals(3, arrrs.getInt(1));
    assertEquals(3, arrrs.getInt(2));
    assertTrue(!arrrs.next());
    assertTrue(arrrs.previous());
    assertEquals(3, arrrs.getInt(2));
    arrrs.first();
    assertEquals(1, arrrs.getInt(2));
    arrrs.close();

    arr = rs.getArray(2);
    assertEquals(Types.NUMERIC, arr.getBaseType());
    arrrs = arr.getResultSet();
    assertTrue(arrrs.next());
    assertEquals(new BigDecimal("3.1"), arrrs.getBigDecimal(2));
    assertTrue(arrrs.next());
    assertEquals(new BigDecimal("1.4"), arrrs.getBigDecimal(2));
    arrrs.close();

    arr = rs.getArray(3);
    assertEquals(Types.VARCHAR, arr.getBaseType());
    arrrs = arr.getResultSet(2, 2);
    assertTrue(arrrs.next());
    assertEquals(2, arrrs.getInt(1));
    assertEquals("f'a", arrrs.getString(2));
    assertTrue(arrrs.next());
    assertEquals(3, arrrs.getInt(1));
    assertEquals("fa\"b", arrrs.getString(2));
    assertTrue(!arrrs.next());
    arrrs.close();

    rs.close();
    stmt.close();
  }

  @Test
  public void testSetArray() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet arrRS = stmt.executeQuery("SELECT '{1,2,3}'::int4[]");
    assertTrue(arrRS.next());
    Array arr = arrRS.getArray(1);
    arrRS.close();
    stmt.close();

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest(intarr) VALUES (?)");
    pstmt.setArray(1, arr);
    pstmt.executeUpdate();

    pstmt.setObject(1, arr, Types.ARRAY);
    pstmt.executeUpdate();

    pstmt.setObject(1, arr);
    pstmt.executeUpdate();

    pstmt.close();

    Statement select = conn.createStatement();
    ResultSet rs = select.executeQuery("SELECT intarr FROM arrtest");
    int resultCount = 0;
    while (rs.next()) {
      resultCount++;
      Array result = rs.getArray(1);
      assertEquals(Types.INTEGER, result.getBaseType());
      assertEquals("int4", result.getBaseTypeName());

      Integer[] intarr = (Integer[]) result.getArray();
      assertEquals(3, intarr.length);
      assertEquals(1, intarr[0].intValue());
      assertEquals(2, intarr[1].intValue());
      assertEquals(3, intarr[2].intValue());
    }
    assertEquals(3, resultCount);
  }

  /**
   * Starting with 8.0 non-standard (beginning index isn't 1) bounds the
   * dimensions are returned in the data. The following should return
   * "[0:3]={0,1,2,3,4}" when queried. Older versions simply do not return the
   * bounds.
   */
  @Test
  public void testNonStandardBounds() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("INSERT INTO arrtest (intarr) VALUES ('{1,2,3}')");
    stmt.executeUpdate("UPDATE arrtest SET intarr[0] = 0");
    ResultSet rs = stmt.executeQuery("SELECT intarr FROM arrtest");
    assertTrue(rs.next());
    Array result = rs.getArray(1);
    Integer[] intarr = (Integer[]) result.getArray();
    assertEquals(4, intarr.length);
    for (int i = 0; i < intarr.length; i++) {
      assertEquals(i, intarr[i].intValue());
    }
  }

  @Test
  public void testMultiDimensionalArray() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]");
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    Object[] oa = (Object[]) arr.getArray();
    assertEquals(2, oa.length);
    Integer[] i0 = (Integer[]) oa[0];
    assertEquals(2, i0.length);
    assertEquals(1, i0[0].intValue());
    assertEquals(2, i0[1].intValue());
    Integer[] i1 = (Integer[]) oa[1];
    assertEquals(2, i1.length);
    assertEquals(3, i1[0].intValue());
    assertEquals(4, i1[1].intValue());
    rs.close();
    stmt.close();
  }

  @Test
  public void testNullValues() throws SQLException {

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT ARRAY[1,NULL,3]");
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    Integer[] i = (Integer[]) arr.getArray();
    assertEquals(3, i.length);
    assertEquals(1, i[0].intValue());
    assertNull(i[1]);
    assertEquals(3, i[2].intValue());
  }

  @Test
  public void testUnknownArrayType() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT relacl FROM pg_class WHERE relacl IS NOT NULL LIMIT 1");
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals(Types.ARRAY, rsmd.getColumnType(1));

    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    assertEquals("aclitem", arr.getBaseTypeName());

    ResultSet arrRS = arr.getResultSet();
    ResultSetMetaData arrRSMD = arrRS.getMetaData();
    assertEquals("aclitem", arrRSMD.getColumnTypeName(2));
  }

  @Test
  public void testRecursiveResultSets() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]");
    assertTrue(rs.next());
    Array arr = rs.getArray(1);

    ResultSet arrRS = arr.getResultSet();
    ResultSetMetaData arrRSMD = arrRS.getMetaData();
    assertEquals(Types.ARRAY, arrRSMD.getColumnType(2));
    assertEquals("_int4", arrRSMD.getColumnTypeName(2));

    assertTrue(arrRS.next());
    assertEquals(1, arrRS.getInt(1));
    Array a1 = arrRS.getArray(2);
    ResultSet a1RS = a1.getResultSet();
    ResultSetMetaData a1RSMD = a1RS.getMetaData();
    assertEquals(Types.INTEGER, a1RSMD.getColumnType(2));
    assertEquals("int4", a1RSMD.getColumnTypeName(2));

    assertTrue(a1RS.next());
    assertEquals(1, a1RS.getInt(2));
    assertTrue(a1RS.next());
    assertEquals(2, a1RS.getInt(2));
    assertTrue(!a1RS.next());
    a1RS.close();

    assertTrue(arrRS.next());
    assertEquals(2, arrRS.getInt(1));
    Array a2 = arrRS.getArray(2);
    ResultSet a2RS = a2.getResultSet();

    assertTrue(a2RS.next());
    assertEquals(3, a2RS.getInt(2));
    assertTrue(a2RS.next());
    assertEquals(4, a2RS.getInt(2));
    assertTrue(!a2RS.next());
    a2RS.close();

    arrRS.close();
    rs.close();
    stmt.close();
  }

  @Test
  public void testNullString() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '{a,NULL}'::text[]");
    assertTrue(rs.next());
    Array arr = rs.getArray(1);

    String[] s = (String[]) arr.getArray();
    assertEquals(2, s.length);
    assertEquals("a", s[0]);
    assertNull(s[1]);
  }

  @Test
  public void testEscaping() throws SQLException {
    Statement stmt = conn.createStatement();
    String sql = "SELECT E'{{c\\\\\"d, ''}, {\"\\\\\\\\\",\"''\"}}'::text[]";

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next());

    Array arr = rs.getArray(1);
    String[][] s = (String[][]) arr.getArray();
    assertEquals("c\"d", s[0][0]);
    assertEquals("'", s[0][1]);
    assertEquals("\\", s[1][0]);
    assertEquals("'", s[1][1]);

    ResultSet arrRS = arr.getResultSet();

    assertTrue(arrRS.next());
    Array a1 = arrRS.getArray(2);
    ResultSet rs1 = a1.getResultSet();
    assertTrue(rs1.next());
    assertEquals("c\"d", rs1.getString(2));
    assertTrue(rs1.next());
    assertEquals("'", rs1.getString(2));
    assertTrue(!rs1.next());

    assertTrue(arrRS.next());
    Array a2 = arrRS.getArray(2);
    ResultSet rs2 = a2.getResultSet();
    assertTrue(rs2.next());
    assertEquals("\\", rs2.getString(2));
    assertTrue(rs2.next());
    assertEquals("'", rs2.getString(2));
    assertTrue(!rs2.next());
  }

  @Test
  public void testWriteMultiDimensional() throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]");
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    rs.close();
    stmt.close();

    String sql = "SELECT ?::int[]";

    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setArray(1, arr);
    rs = pstmt.executeQuery();
    assertTrue(rs.next());
    arr = rs.getArray(1);

    Integer[][] i = (Integer[][]) arr.getArray();
    assertEquals(1, i[0][0].intValue());
    assertEquals(2, i[0][1].intValue());
    assertEquals(3, i[1][0].intValue());
    assertEquals(4, i[1][1].intValue());
    pstmt.close();
  }

  @Test
  public void testCreateArrayOfInt() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("SELECT ?::int[]");
    Integer[] in = new Integer[3];
    in[0] = 0;
    in[1] = -1;
    in[2] = 2;
    pstmt.setArray(1, conn.createArrayOf("int4", in));

    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    Integer[] out = (Integer[]) arr.getArray();

    assertEquals(3, out.length);
    assertEquals(0, out[0].intValue());
    assertEquals(-1, out[1].intValue());
    assertEquals(2, out[2].intValue());
    pstmt.close();
  }

  @Test
  public void testCreateArrayOfMultiString() throws SQLException {
    PreparedStatement pstmt = conn.prepareStatement("SELECT ?::text[]");
    String[][] in = new String[2][2];
    in[0][0] = "a";
    in[0][1] = "";
    in[1][0] = "\\";
    in[1][1] = "\"\\'z";
    pstmt.setArray(1, conn.createArrayOf("text", in));

    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    String[][] out = (String[][]) arr.getArray();

    assertEquals(2, out.length);
    assertEquals(2, out[0].length);
    assertEquals("a", out[0][0]);
    assertEquals("", out[0][1]);
    assertEquals("\\", out[1][0]);
    assertEquals("\"\\'z", out[1][1]);
    pstmt.close();
  }

  @Test
  public void testCreateArrayOfNull() throws SQLException {

    String sql = "SELECT ?::int8[]";

    PreparedStatement pstmt = conn.prepareStatement(sql);
    String[] in = new String[2];
    in[0] = null;
    in[1] = null;
    pstmt.setArray(1, conn.createArrayOf("int8", in));

    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    Long[] out = (Long[]) arr.getArray();

    assertEquals(2, out.length);
    assertNull(out[0]);
    assertNull(out[1]);
    pstmt.close();
  }

  @Test
  public void testCreateEmptyArrayOfIntViaAlias() throws SQLException {

    PreparedStatement pstmt = conn.prepareStatement("SELECT ?::int[]");
    Integer[] in = new Integer[0];
    pstmt.setArray(1, conn.createArrayOf("integer", in));

    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    Array arr = rs.getArray(1);
    Integer[] out = (Integer[]) arr.getArray();

    assertEquals(0, out.length);

    ResultSet arrRs = arr.getResultSet();
    assertFalse(arrRs.next());
    pstmt.close();
  }

  @Test
  public void testCreateArrayWithoutServer() throws SQLException {
    String[][] in = new String[2][2];
    in[0][0] = "a";
    in[0][1] = "";
    in[1][0] = "\\";
    in[1][1] = "\"\\'z";

    Array arr = conn.createArrayOf("varchar", in);
    String[][] out = (String[][]) arr.getArray();

    assertEquals(2, out.length);
    assertEquals(2, out[0].length);
    assertEquals("a", out[0][0]);
    assertEquals("", out[0][1]);
    assertEquals("\\", out[1][0]);
    assertEquals("\"\\'z", out[1][1]);
  }

  @Test
  public void testCreatePrimitiveArray() throws SQLException {
    double[][] in = new double[2][2];
    in[0][0] = 3.5;
    in[0][1] = -4.5;
    in[1][0] = 10.0 / 3;
    in[1][1] = 77;

    Array arr = conn.createArrayOf("float8", in);
    Double[][] out = (Double[][]) arr.getArray();

    assertEquals(2, out.length);
    assertEquals(2, out[0].length);
    assertEquals(3.5, out[0][0], 0.00001);
    assertEquals(-4.5, out[0][1], 0.00001);
    assertEquals(10.0 / 3, out[1][0], 0.00001);
    assertEquals(77, out[1][1], 0.00001);
  }

  @Test
  public void testSetObjectFromJavaArray() throws SQLException {
    String[] strArray = new String[] {"a", "b", "c"};

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest(strarr) VALUES (?)");

    // Correct way, though the use of "text" as a type is non-portable.
    // Only supported for JDK 1.6 and JDBC4
    Array sqlArray = conn.createArrayOf("text", strArray);
    pstmt.setArray(1, sqlArray);
    pstmt.executeUpdate();

    /*
     * The original driver reasons these 2 tests should fail but we support
     * them; and supporting them doesn't cause any JDBC spec invalidation.
     */

    // Incorrect, but commonly attempted by many ORMs:
    pstmt.setObject(1, strArray, Types.ARRAY);
    pstmt.executeUpdate();

    // Also incorrect, but commonly attempted by many ORMs:
    pstmt.setObject(1, strArray);
    pstmt.executeUpdate();

    pstmt.close();
  }

}
