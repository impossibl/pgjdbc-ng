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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



/*
 * CallableStatement tests.
 * @author Paul Bethe
 */
@RunWith(JUnit4.class)
public class CallableStatementTest {

  private Connection con;

  @Before
  public void setUp() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "int_table", "id int");
    Statement stmt = con.createStatement();
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getString (varchar) "
        + "RETURNS varchar AS ' DECLARE inString alias for $1; begin "
        + "return ''bob''; end; ' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getDouble (float) "
        + "RETURNS float AS ' DECLARE inString alias for $1; begin "
        + "return 42.42; end; ' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getVoid (float) "
        + "RETURNS void AS ' DECLARE inString alias for $1; begin "
        + " return; end; ' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getInt (int) RETURNS int "
        + " AS 'DECLARE inString alias for $1; begin "
        + "return 42; end;' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getShort (int2) RETURNS int2 "
        + " AS 'DECLARE inString alias for $1; begin "
        + "return 42; end;' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getNumeric (numeric) "
        + "RETURNS numeric AS ' DECLARE inString alias for $1; "
        + "begin return 42; end; ' LANGUAGE plpgsql;");

    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getNumericWithoutArg() "
        + "RETURNS numeric AS '  "
        + "begin return 42; end; ' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getarray() RETURNS int[] as 'SELECT ''{1,2}''::int[];' LANGUAGE sql");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__raisenotice() RETURNS int as 'BEGIN RAISE NOTICE ''hello'';  RAISE NOTICE ''goodbye''; RETURN 1; END;' LANGUAGE plpgsql");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__insertInt(int) RETURNS int as 'BEGIN INSERT INTO int_table(id) VALUES ($1); RETURN 1; END;' LANGUAGE plpgsql");


    stmt.execute("create temp table numeric_tab (MAX_VAL NUMERIC(30,15), MIN_VAL NUMERIC(30,15), NULL_VAL NUMERIC(30,15) NULL)");
    stmt.execute("insert into numeric_tab values ( 999999999999999,0.000000000000001, null)");
    stmt.execute("CREATE OR REPLACE FUNCTION myiofunc(a INOUT int, b OUT int) AS 'BEGIN b := a; a := 1; END;' LANGUAGE plpgsql");
    stmt.execute("CREATE OR REPLACE FUNCTION myif(a INOUT int, b IN int) AS 'BEGIN a := b; END;' LANGUAGE plpgsql");

    stmt.execute("create or replace function "
                                         + "Numeric_Proc( OUT IMAX NUMERIC(30,15), OUT IMIN NUMERIC(30,15), OUT INUL NUMERIC(30,15))  as "
                                         + "'begin "
                                         +         "select max_val into imax from numeric_tab;"
                                         +         "select min_val into imin from numeric_tab;"
                                         +         "select null_val into inul from numeric_tab;"

                                         + " end;' "
                                         + "language plpgsql;");

    stmt.execute("CREATE OR REPLACE FUNCTION test_somein_someout("
            + "pa IN int4,"
            + "pb OUT varchar,"
            + "pc OUT int8)"
            + " AS "
            + "'begin "
            + "pb := ''out'';"
            + "pc := pa + 1;"
            + "end;'"
            + "LANGUAGE plpgsql VOLATILE;");
    stmt.execute("CREATE OR REPLACE FUNCTION test_somein_someout2("
        + "pb OUT varchar,"
        + "pc OUT int8,"
        + "pa IN int4)"
        + " AS "
        + "'begin "
        + "pb := ''out'';"
        + "pc := pa + 1;"
        + "end;'"
        + "LANGUAGE plpgsql VOLATILE;");
    stmt.execute("CREATE OR REPLACE FUNCTION test_somein_someout3("
        + "pb OUT varchar,"
        + "pa IN int4,"
        + "pc OUT int8)"
        + " AS "
        + "'begin "
        + "pb := ''out'';"
        + "pc := pa + 1;"
        + "end;'"
        + "LANGUAGE plpgsql VOLATILE;");
    stmt.execute("CREATE OR REPLACE FUNCTION test_allinout("
            + "pa INOUT int4,"
            + "pb INOUT varchar,"
            + "pc INOUT int8)"
            + " AS "
            + "'begin "
            + "pa := pa + 1;"
            + "pb := ''foo out'';"
            + "pc := pa + 1;"
            + "end;'"
            + "LANGUAGE plpgsql VOLATILE;");

    stmt.close();
  }

  @After
  public void tearDown() throws Exception {
    Statement stmt = con.createStatement();
    TestUtil.dropTable(con, "int_table");
    stmt.execute("drop FUNCTION testspg__getString (varchar);");
    stmt.execute("drop FUNCTION testspg__getDouble (float);");
    stmt.execute("drop FUNCTION testspg__getVoid(float);");
    stmt.execute("drop FUNCTION testspg__getInt (int);");
    stmt.execute("drop FUNCTION testspg__getShort(int2)");
    stmt.execute("drop FUNCTION testspg__getNumeric (numeric);");

    stmt.execute("drop FUNCTION testspg__getNumericWithoutArg ();");
    stmt.execute("DROP FUNCTION testspg__getarray();");
    stmt.execute("DROP FUNCTION testspg__raisenotice();");
    stmt.execute("DROP FUNCTION testspg__insertInt(int);");
    stmt.close();
    TestUtil.closeDB(con);
  }

  final String func = "{ ? = call ";
  final String pkgName = "testspg__";

  @Test
  public void testGetUpdateCount() throws SQLException {
    CallableStatement call = con.prepareCall(func + pkgName + "getDouble (?) }");
    call.setDouble(2, 3.04);
    call.registerOutParameter(1, Types.DOUBLE);
    call.execute();
    assertEquals(-1, call.getUpdateCount());
    assertNull(call.getResultSet());
    assertEquals(42.42, call.getDouble(1), 0.00001);
    call.close();

    // test without an out parameter
    call = con.prepareCall("{ call " + pkgName + "getDouble(?) }");
    call.setDouble(1, 3.04);
    call.execute();
    assertEquals(-1, call.getUpdateCount());
    ResultSet rs = call.getResultSet();
    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals(42.42, rs.getDouble(1), 0.00001);
    assertTrue(!rs.next());
    rs.close();

    assertEquals(-1, call.getUpdateCount());
    assertTrue(!call.getMoreResults());
    call.close();
  }

  @Test
  public void testGetDouble() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getDouble (?) }");
    call.setDouble(2, 3.04);
    call.registerOutParameter(1, Types.DOUBLE);
    call.execute();
    assertEquals(42.42, call.getDouble(1), 0.00001);
    call.close();

    // test without an out parameter
    call = con.prepareCall("{ call " + pkgName + "getDouble(?) }");
    call.setDouble(1, 3.04);
    call.execute();
    call.close();

    call = con.prepareCall("{ call " + pkgName + "getVoid(?) }");
    call.setDouble(1, 3.04);
    call.execute();
    call.close();
  }

  @Test
  public void testGetInt() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getInt (?) }");
    call.setInt(2, 4);
    call.registerOutParameter(1, Types.INTEGER);
    call.execute();
    assertEquals(42, call.getInt(1));
    call.close();
  }

  @Test
  public void testGetShort() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getShort (?) }");
    call.setShort(2, (short) 4);
    call.registerOutParameter(1, Types.SMALLINT);
    call.execute();
    assertEquals(42, call.getShort(1));
    call.close();
  }

  @Test
  public void testGetNumeric() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getNumeric (?) }");
    call.setBigDecimal(2, new java.math.BigDecimal(4));
    call.registerOutParameter(1, Types.NUMERIC);
    call.execute();
    assertEquals(new java.math.BigDecimal(42), call.getBigDecimal(1));
    call.close();
  }

  @Test
  public void testGetNumericWithoutArg() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getNumericWithoutArg () }");
    call.registerOutParameter(1, Types.NUMERIC);
    call.execute();
    assertEquals(new java.math.BigDecimal(42), call.getBigDecimal(1));
    call.close();
  }

  @Test
  public void testGetString() throws Throwable {
    CallableStatement call = con.prepareCall(func + pkgName + "getString (?) }");
    call.setString(2, "foo");
    call.registerOutParameter(1, Types.VARCHAR);
    call.execute();
    assertEquals("bob", call.getString(1));
    call.close();
  }

  @Test
  public void testGetArray() throws SQLException {
    CallableStatement call = con.prepareCall(func + pkgName + "getarray()}");
    call.registerOutParameter(1, Types.ARRAY);
    call.execute();
    Array arr = call.getArray(1);
    ResultSet rs = arr.getResultSet();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(!rs.next());
    rs.close();
    call.close();
  }

  @Test
  public void testRaiseNotice() throws SQLException {
    CallableStatement call = con.prepareCall(func + pkgName + "raisenotice()}");
    call.registerOutParameter(1, Types.INTEGER);
    call.execute();
    SQLWarning warn = call.getWarnings();
    assertNotNull(warn);
    assertEquals("hello", warn.getMessage());
    warn = warn.getNextWarning();
    assertNotNull(warn);
    assertEquals("goodbye", warn.getMessage());
    assertEquals(1, call.getInt(1));
    call.close();
  }

  @Test
  public void testWasNullBeforeFetch() throws SQLException {
    CallableStatement cs = con.prepareCall("{? = call lower(?)}");
    cs.registerOutParameter(1, Types.VARCHAR);
    cs.setString(2, "Hi");
    try {
      cs.wasNull();
      fail("expected exception");
    }
    catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
    cs.close();
  }

  @Test
  public void testFetchBeforeExecute() throws SQLException {
    CallableStatement cs = con.prepareCall("{? = call lower(?)}");
    cs.registerOutParameter(1, Types.VARCHAR);
    cs.setString(2, "Hi");
    try {
      cs.getString(1);
      fail("expected exception");
    }
    catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
    cs.close();
  }

  @Test
  public void testFetchWithNoResults() throws SQLException {
    CallableStatement cs = con.prepareCall("{call now()}");
    cs.execute();
    try {
      cs.getObject(1);
      fail("expected exception");
    }
    catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
    cs.close();
  }

  @Test
  public void testBadStmt() throws Throwable {
    tryOneBadStmt("{ ?= " + pkgName + "getString (?) }");
    tryOneBadStmt("{ ?= call getString (?) ");
    tryOneBadStmt("{ = ? call getString (?); }");
  }

  protected void tryOneBadStmt(String sql) throws SQLException {
    try (Statement cs = con.prepareCall(sql)) {
      fail("Bad statement (" + sql + ") was not caught.");
    }
    catch (SQLException e) {
      // Expected...
    }
  }

  @Test
  public void testBatchCall() throws SQLException {
    CallableStatement call = con.prepareCall("{ call " + pkgName + "insertInt(?) }");
    call.setInt(1, 1);
    call.addBatch();
    call.setInt(1, 2);
    call.addBatch();
    call.setInt(1, 3);
    call.addBatch();
    call.executeBatch();
    call.close();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM int_table ORDER BY id");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testSomeInOut() throws Throwable {
    CallableStatement call = con.prepareCall("{ call test_somein_someout(?,?,?) }");

    call.setInt(1, 20);
    call.registerOutParameter(2, Types.VARCHAR);
    call.registerOutParameter(3, Types.BIGINT);
    call.execute();
    Assert.assertEquals("out", call.getString(2));
    Assert.assertEquals(21, call.getInt(3));
    call.close();
  }

  @Test
  public void testSomeInOut2() throws Throwable {
    CallableStatement call = con.prepareCall("{ call test_somein_someout2(?,?,?) }");

    call.registerOutParameter(1, Types.VARCHAR);
    call.registerOutParameter(2, Types.BIGINT);
    call.setInt(3, 20);
    call.execute();
    Assert.assertEquals("out", call.getString(1));
    Assert.assertEquals(21, call.getInt(2));
    call.close();
  }

  @Test
  public void testSomeInOut3() throws Throwable {
    CallableStatement call = con.prepareCall("{ call test_somein_someout3(?,?,?) }");

    call.registerOutParameter(1, Types.VARCHAR);
    call.setInt(2, 20);
    call.registerOutParameter(3, Types.BIGINT);
    call.execute();
    Assert.assertEquals("out", call.getString(1));
    Assert.assertEquals(21, call.getInt(3));
    call.close();
  }

  @Test
  public void testNotEnoughParameters() throws Throwable {

    CallableStatement cs = con.prepareCall("{call myiofunc(?,?)}");
    cs.setInt(1, 2);
    cs.registerOutParameter(2, Types.INTEGER);
    try {
      cs.execute();
      fail("Should throw an exception ");
    }
    catch (SQLException ex) {
      // Expected...
    }
    cs.close();
  }

  @Test
  public void testTooManyParameters() throws Throwable {

    CallableStatement cs = con.prepareCall("{call myif(?,?)}");
    try {
      cs.setInt(1, 1);
      cs.setInt(2, 2);
      cs.registerOutParameter(1, Types.INTEGER);
      cs.registerOutParameter(2, Types.INTEGER);
      cs.execute();
      fail("should throw an exception");
    }
    catch (SQLException ex) {
      // Expected...
    }
    cs.close();
  }

  @Test
  public void testAllInOut() throws Throwable {

    CallableStatement call = con.prepareCall("{ call test_allinout(?,?,?) }");

    call.registerOutParameter(1, Types.INTEGER);
    call.registerOutParameter(2, Types.VARCHAR);
    call.registerOutParameter(3, Types.BIGINT);
    call.setInt(1, 20);
    call.setString(2, "hi");
    call.setInt(3, 123);
    call.execute();
    call.getInt(1);
    call.getString(2);
    call.getLong(3);

    call.close();
  }

  @Test
  public void testNumeric() throws Throwable {

    CallableStatement call = con.prepareCall("{ call Numeric_Proc(?,?,?) }");

    call.registerOutParameter(1, Types.NUMERIC, 15);
    call.registerOutParameter(2, Types.NUMERIC, 15);
    call.registerOutParameter(3, Types.NUMERIC, 15);

    call.executeUpdate();
    java.math.BigDecimal ret = call.getBigDecimal(1);
    assertTrue("correct return from getNumeric () should be 999999999999999.000000000000000 but returned " + ret.toString(),
        ret.equals(new java.math.BigDecimal("999999999999999.000000000000000")));

    ret = call.getBigDecimal(2);
    assertTrue("correct return from getNumeric ()", ret.equals(new java.math.BigDecimal("0.000000000000001")));
    try {
      ret = call.getBigDecimal(3);
    }
    catch (NullPointerException ex) {
      assertTrue("This should be null", call.wasNull());
    }

    call.close();
  }

  @Test
  public void testGetObjectDecimal() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table decimal_tab ( max_val numeric(30,15), min_val numeric(30,15), nul_val numeric(30,15) )");
      stmt.execute("insert into decimal_tab values (999999999999999.000000000000000,0.000000000000001,null)");
      stmt.execute("create or replace function "
          + "decimal_proc( OUT pmax numeric, OUT pmin numeric, OUT nval numeric)  as "
          + "'begin "
          + "select max_val into pmax from decimal_tab;"
          + "select min_val into pmin from decimal_tab;"
          + "select nul_val into nval from decimal_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call decimal_proc(?,?,?) }");
      cstmt.registerOutParameter(1, Types.DECIMAL);
      cstmt.registerOutParameter(2, Types.DECIMAL);
      cstmt.registerOutParameter(3, Types.DECIMAL);
      cstmt.executeUpdate();
      BigDecimal val = (BigDecimal) cstmt.getObject(1);
      assertTrue(val.compareTo(new BigDecimal("999999999999999.000000000000000")) == 0);
      val = (BigDecimal) cstmt.getObject(2);
      assertTrue(val.compareTo(new BigDecimal("0.000000000000001")) == 0);
      val = (BigDecimal) cstmt.getObject(3);
      assertTrue(val == null);
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function decimal_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testVarcharBool() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table vartab( max_val text, min_val text)");
      stmt.execute("insert into vartab values ('a','b')");
      stmt.execute("create or replace function "
          + "updatevarchar( in imax text, in imin text)  returns int as "
          + "'begin " + "update vartab set max_val = imax;"
          + "update vartab set min_val = imin;" + "return 0;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call updatevarchar(?,?) }");
      cstmt.setObject(1, Boolean.TRUE, Types.VARCHAR);
      cstmt.setObject(2, Boolean.FALSE, Types.VARCHAR);

      cstmt.executeUpdate();
      cstmt.close();

      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("select * from vartab");
      assertTrue(rs.next());
      assertTrue(rs.getString(1).equals(Boolean.TRUE.toString()));

      assertTrue(rs.getString(2).equals(Boolean.FALSE.toString()));
      rs.close();
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function updatevarchar(text,text)");
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testInOut() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createBitTab);
      stmt.execute(insertBitTab);
      stmt.execute("create or replace function "
          + "insert_bit( inout IMAX boolean, inout IMIN boolean, inout INUL boolean)  as "
          + "'begin "
          + "insert into bit_tab values( imax, imin, inul);"
          + "select max_val into imax from bit_tab;"
          + "select min_val into imin from bit_tab;"
          + "select null_val into inul from bit_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call insert_bit(?,?,?) }");
      cstmt.setObject(1, "true", Types.BIT);
      cstmt.setObject(2, "false", Types.BIT);
      cstmt.setNull(3, Types.BIT);
      cstmt.registerOutParameter(1, Types.BIT);
      cstmt.registerOutParameter(2, Types.BIT);
      cstmt.registerOutParameter(3, Types.BIT);
      cstmt.executeUpdate();

      assertTrue(cstmt.getBoolean(1) == true);
      assertTrue(cstmt.getBoolean(2) == false);
      cstmt.getBoolean(3);
      assertTrue(cstmt.wasNull());

      cstmt.close();
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function insert_bit(boolean, boolean, boolean)");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  private final String createBitTab = "create temp table bit_tab ( max_val boolean, min_val boolean, null_val boolean )";
  private final String insertBitTab = "insert into bit_tab values (true,false,null)";

  @Test
  public void testSetObjectBit() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createBitTab);
      stmt.execute(insertBitTab);
      stmt.execute("create or replace function "
          + "update_bit( in IMAX boolean, in IMIN boolean, in INUL boolean) returns int as "
          + "'begin "
          + "update bit_tab set  max_val = imax;"
          + "update bit_tab set  min_val = imin;"
          + "update bit_tab set  min_val = inul;"
          + " return 0;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call update_bit(?,?,?) }");
      cstmt.setObject(1, "true", Types.BIT);
      cstmt.setObject(2, "false", Types.BIT);
      cstmt.setNull(3, Types.BIT);
      cstmt.executeUpdate();
      cstmt.close();

      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("select * from bit_tab");

      assertTrue(rs.next());
      assertTrue(rs.getBoolean(1) == true);
      assertTrue(rs.getBoolean(2) == false);
      rs.getBoolean(3);
      assertTrue(rs.wasNull());

      rs.close();
      stmt.close();
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function update_bit(boolean, boolean, boolean)");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetObjectLongVarchar() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table longvarchar_tab ( t text, null_val text )");
      stmt.execute("insert into longvarchar_tab values ('testdata',null)");
      stmt.execute("create or replace function "
          + "longvarchar_proc( OUT pcn text, OUT nval text)  as "
          + "'begin "
          + "select t into pcn from longvarchar_tab;"
          + "select null_val into nval from longvarchar_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.execute("create or replace function "
          + "lvarchar_in_name( IN pcn text) returns int as "
          + "'begin "
          + "update longvarchar_tab set t=pcn;"
          + "return 0;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call longvarchar_proc(?,?) }");
      cstmt.registerOutParameter(1, Types.LONGVARCHAR);
      cstmt.registerOutParameter(2, Types.LONGVARCHAR);
      cstmt.executeUpdate();
      String val = (String) cstmt.getObject(1);
      assertTrue(val.equals("testdata"));
      val = (String) cstmt.getObject(2);
      assertTrue(val == null);
      cstmt.close();
      cstmt = con.prepareCall("{ call lvarchar_in_name(?) }");
      String maxFloat = "3.4E38";
      cstmt.setObject(1, new Float(maxFloat), Types.LONGVARCHAR);
      cstmt.executeUpdate();
      cstmt.close();
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("select * from longvarchar_tab");
      assertTrue(rs.next());
      String rval = (String) rs.getObject(1);
      assertEquals(rval.trim(), maxFloat.trim());
      rs.close();
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function longvarchar_proc()");
        dstmt.execute("drop function lvarchar_in_name(text)");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetBytes01() throws Throwable {
    byte[] testdata = "TestData".getBytes();
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table varbinary_tab ( vbinary bytea, null_val bytea )");
      stmt.execute("create or replace function "
          + "varbinary_proc( OUT pcn bytea, OUT nval bytea)  as "
          + "'begin " + "select vbinary into pcn from varbinary_tab;"
          + "select null_val into nval from varbinary_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
      PreparedStatement pstmt = con.prepareStatement("insert into varbinary_tab values (?,?)");
      pstmt.setBytes(1, testdata);
      pstmt.setBytes(2, null);

      pstmt.executeUpdate();
      pstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call varbinary_proc(?,?) }");
      cstmt.registerOutParameter(1, Types.VARBINARY);
      cstmt.registerOutParameter(2, Types.VARBINARY);
      cstmt.executeUpdate();
      byte[] retval = cstmt.getBytes(1);
      for (int i = 0; i < testdata.length; i++) {
        assertTrue(testdata[i] == retval[i]);
      }

      retval = cstmt.getBytes(2);
      assertTrue(retval == null);

      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function varbinary_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  private final String createDecimalTab = "create temp table decimal_tab ( max_val float, min_val float, null_val float )";
  private final String insertDecimalTab = "insert into decimal_tab values (1.0E125,1.0E-130,null)";
  private final String createFloatProc = "create or replace function "
      + "float_proc( OUT IMAX float, OUT IMIN float, OUT INUL float)  as "
      + "'begin "
      + "select max_val into imax from decimal_tab;"
      + "select min_val into imin from decimal_tab;"
      + "select null_val into inul from decimal_tab;"
      + " end;' "
      + "language plpgsql;";
  private final String createUpdateFloat = "create or replace function "
      + "updatefloat_proc ( IN maxparm float, IN minparm float ) returns int as "
      + "'begin "
      + "update decimal_tab set max_val=maxparm;"
      + "update decimal_tab set min_val=minparm;"
      + "return 0;"
      + " end;' "
      + "language plpgsql;";
  private final String createRealTab = "create temp table real_tab ( max_val float(25), min_val float(25), null_val float(25) )";
  private final String insertRealTab = "insert into real_tab values (1.0E37,1.0E-37, null)";
  private final String dropFloatProc = "drop function float_proc()";
  private final String createUpdateReal = "create or replace function "
      + "update_real_proc ( IN maxparm float(25), IN minparm float(25) ) returns int as "
      + "'begin "
      + "update real_tab set max_val=maxparm;"
      + "update real_tab set min_val=minparm;"
      + "return 0;"
      + " end;' "
      + "language plpgsql;";
  private final String dropUpdateReal = "drop function update_real_proc(float, float)";
  private final double[] doubleValues = {1.0E125, 1.0E-130};
  private final int[] intValues = {2147483647, -2147483648};

  @Test
  public void testUpdateReal() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createRealTab);
      stmt.execute(createUpdateReal);
      stmt.execute(insertRealTab);
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call update_real_proc(?,?) }");
      BigDecimal val = new BigDecimal(intValues[0]);
      val.floatValue();
      cstmt.setObject(1, val, Types.REAL);
      val = new BigDecimal(intValues[1]);
      cstmt.setObject(2, val, Types.REAL);
      cstmt.executeUpdate();
      cstmt.close();
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("select * from real_tab");
      assertTrue(rs.next());
      Float oVal = new Float(intValues[0]);
      Float rVal = new Float(rs.getObject(1).toString());
      assertTrue(oVal.equals(rVal));
      oVal = new Float(intValues[1]);
      rVal = new Float(rs.getObject(2).toString());
      assertTrue(oVal.equals(rVal));
      rs.close();
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute(dropUpdateReal);
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testUpdateDecimal() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createDecimalTab);
      stmt.execute(createUpdateFloat);
      stmt.close();
      PreparedStatement pstmt = con.prepareStatement("insert into decimal_tab values (?,?)");
      // note these are reversed on purpose
      pstmt.setDouble(1, doubleValues[1]);
      pstmt.setDouble(2, doubleValues[0]);

      pstmt.executeUpdate();
      pstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call updatefloat_proc(?,?) }");
      cstmt.setDouble(1, doubleValues[0]);
      cstmt.setDouble(2, doubleValues[1]);
      cstmt.executeUpdate();
      cstmt.close();
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery("select * from decimal_tab");
      assertTrue(rs.next());
      assertTrue(rs.getDouble(1) == doubleValues[0]);
      assertTrue(rs.getDouble(2) == doubleValues[1]);
      rs.close();
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function updatefloat_proc(float, float)");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetBytes02() throws Throwable {
    byte[] testdata = "TestData".getBytes();
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table longvarbinary_tab ( vbinary bytea, null_val bytea )");
      stmt.execute("create or replace function "
          + "longvarbinary_proc( OUT pcn bytea, OUT nval bytea)  as "
          + "'begin "
          + "select vbinary into pcn from longvarbinary_tab;"
          + "select null_val into nval from longvarbinary_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
      PreparedStatement pstmt = con.prepareStatement("insert into longvarbinary_tab values (?,?)");
      pstmt.setBytes(1, testdata);
      pstmt.setBytes(2, null);

      pstmt.executeUpdate();
      pstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call longvarbinary_proc(?,?) }");
      cstmt.registerOutParameter(1, Types.LONGVARBINARY);
      cstmt.registerOutParameter(2, Types.LONGVARBINARY);
      cstmt.executeUpdate();
      byte[] retval = cstmt.getBytes(1);
      for (int i = 0; i < testdata.length; i++) {
        assertTrue(testdata[i] == retval[i]);
      }

      retval = cstmt.getBytes(2);
      assertTrue(retval == null);

      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function longvarbinary_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetObjectFloat() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createDecimalTab);
      stmt.execute(insertDecimalTab);
      stmt.execute(createFloatProc);
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call float_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.FLOAT);
      cstmt.registerOutParameter(2, java.sql.Types.FLOAT);
      cstmt.registerOutParameter(3, java.sql.Types.FLOAT);
      cstmt.executeUpdate();
      Double val = (Double) cstmt.getObject(1);
      assertTrue(val.doubleValue() == doubleValues[0]);

      val = (Double) cstmt.getObject(2);
      assertTrue(val.doubleValue() == doubleValues[1]);

      val = (Double) cstmt.getObject(3);
      assertTrue(cstmt.wasNull());

      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute(dropFloatProc);
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetDouble01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table d_tab ( max_val float, min_val float, null_val float )");
      stmt.execute("insert into d_tab values (1.0E125,1.0E-130,null)");
      stmt.execute("create or replace function "
          + "double_proc( OUT IMAX float, OUT IMIN float, OUT INUL float)  as "
          + "'begin "
          + "select max_val into imax from d_tab;"
          + "select min_val into imin from d_tab;"
          + "select null_val into inul from d_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call double_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.DOUBLE);
      cstmt.registerOutParameter(2, java.sql.Types.DOUBLE);
      cstmt.registerOutParameter(3, java.sql.Types.DOUBLE);
      cstmt.executeUpdate();
      assertTrue(cstmt.getDouble(1) == 1.0E125);
      assertTrue(cstmt.getDouble(2) == 1.0E-130);
      cstmt.getDouble(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function double_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetDoubleAsReal() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table d_tab ( max_val float, min_val float, null_val float )");
      stmt.execute("insert into d_tab values (3.4E38,1.4E-45,null)");
      stmt.execute("create or replace function "
          + "double_proc( OUT IMAX float, OUT IMIN float, OUT INUL float)  as "
          + "'begin "
          + "select max_val into imax from d_tab;"
          + "select min_val into imin from d_tab;"
          + "select null_val into inul from d_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call double_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.REAL);
      cstmt.registerOutParameter(2, java.sql.Types.REAL);
      cstmt.registerOutParameter(3, java.sql.Types.REAL);
      cstmt.executeUpdate();
      assertTrue(cstmt.getFloat(1) == 3.4E38f);
      assertTrue(cstmt.getFloat(2) == 1.4E-45f);
      cstmt.getFloat(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function double_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetShort01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table short_tab ( max_val int2, min_val int2, null_val int2 )");
      stmt.execute("insert into short_tab values (32767,-32768,null)");
      stmt.execute("create or replace function "
          + "short_proc( OUT IMAX int2, OUT IMIN int2, OUT INUL int2)  as "
          + "'begin "
          + "select max_val into imax from short_tab;"
          + "select min_val into imin from short_tab;"
          + "select null_val into inul from short_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call short_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.SMALLINT);
      cstmt.registerOutParameter(2, java.sql.Types.SMALLINT);
      cstmt.registerOutParameter(3, java.sql.Types.SMALLINT);
      cstmt.executeUpdate();
      assertTrue(cstmt.getShort(1) == 32767);
      assertTrue(cstmt.getShort(2) == -32768);
      cstmt.getShort(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function short_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetInt01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table i_tab ( max_val int, min_val int, null_val int )");
      stmt.execute("insert into i_tab values (2147483647,-2147483648,null)");
      stmt.execute("create or replace function "
          + "int_proc( OUT IMAX int, OUT IMIN int, OUT INUL int)  as "
          + "'begin "
          + "select max_val into imax from i_tab;"
          + "select min_val into imin from i_tab;"
          + "select null_val into inul from i_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call int_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.registerOutParameter(2, java.sql.Types.INTEGER);
      cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
      cstmt.executeUpdate();
      assertTrue(cstmt.getInt(1) == 2147483647);
      assertTrue(cstmt.getInt(2) == -2147483648);
      cstmt.getInt(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function int_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetLong01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table l_tab ( max_val int8, min_val int8, null_val int8 )");
      stmt.execute("insert into l_tab values (9223372036854775807,-9223372036854775808,null)");
      stmt.execute("create or replace function "
          + "bigint_proc( OUT IMAX int8, OUT IMIN int8, OUT INUL int8)  as "
          + "'begin "
          + "select max_val into imax from l_tab;"
          + "select min_val into imin from l_tab;"
          + "select null_val into inul from l_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call bigint_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.BIGINT);
      cstmt.registerOutParameter(2, java.sql.Types.BIGINT);
      cstmt.registerOutParameter(3, java.sql.Types.BIGINT);
      cstmt.executeUpdate();
      assertTrue(cstmt.getLong(1) == 9223372036854775807L);
      assertTrue(cstmt.getLong(2) == -9223372036854775808L);
      cstmt.getLong(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function bigint_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetBoolean01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute(createBitTab);
      stmt.execute(insertBitTab);
      stmt.execute("create or replace function "
          + "bit_proc( OUT IMAX boolean, OUT IMIN boolean, OUT INUL boolean)  as "
          + "'begin "
          + "select max_val into imax from bit_tab;"
          + "select min_val into imin from bit_tab;"
          + "select null_val into inul from bit_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call bit_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.BIT);
      cstmt.registerOutParameter(2, java.sql.Types.BIT);
      cstmt.registerOutParameter(3, java.sql.Types.BIT);
      cstmt.executeUpdate();
      assertTrue(cstmt.getBoolean(1));
      assertTrue(cstmt.getBoolean(2) == false);
      cstmt.getBoolean(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function bit_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testGetByte01() throws Throwable {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create temp table byte_tab ( max_val int2, min_val int2, null_val int2 )");
      stmt.execute("insert into byte_tab values (127,-128,null)");
      stmt.execute("create or replace function "
          + "byte_proc( OUT IMAX int2, OUT IMIN int2, OUT INUL int2)  as "
          + "'begin "
          + "select max_val into imax from byte_tab;"
          + "select min_val into imin from byte_tab;"
          + "select null_val into inul from byte_tab;"
          + " end;' "
          + "language plpgsql;");
      stmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
      throw ex;
    }
    try {
      CallableStatement cstmt = con.prepareCall("{ call byte_proc(?,?,?) }");
      cstmt.registerOutParameter(1, java.sql.Types.TINYINT);
      cstmt.registerOutParameter(2, java.sql.Types.TINYINT);
      cstmt.registerOutParameter(3, java.sql.Types.TINYINT);
      cstmt.executeUpdate();
      assertTrue(cstmt.getByte(1) == 127);
      assertTrue(cstmt.getByte(2) == -128);
      cstmt.getByte(3);
      assertTrue(cstmt.wasNull());
      cstmt.close();
    }
    catch (Exception ex) {
      fail(ex.getMessage());
    }
    finally {
      try {
        Statement dstmt = con.createStatement();
        dstmt.execute("drop function byte_proc()");
        dstmt.close();
      }
      catch (Exception ex) {
        // Expected...
      }
    }
  }

  @Test
  public void testMultipleOutExecutions() throws SQLException {
    CallableStatement cs = con.prepareCall("{call myiofunc(?, ?)}");
    for (int i = 0; i < 10; i++) {
      cs.registerOutParameter(1, Types.INTEGER);
      cs.registerOutParameter(2, Types.INTEGER);
      cs.setInt(1, i);
      cs.execute();
      assertEquals(1, cs.getInt(1));
      assertEquals(i, cs.getInt(2));
      cs.clearParameters();
    }
    cs.close();
  }

  @Test
  public void testCallFunctionWithoutParentheses() throws SQLException {
    CallableStatement cs = con.prepareCall("{?=call current_timestamp}");
    cs = con.prepareCall("{?=call current_timestamp}");
    cs.registerOutParameter(1, Types.TIMESTAMP);
    cs.execute();
    assertNotNull(cs.getTimestamp(1));
    cs.close();
  }
}
