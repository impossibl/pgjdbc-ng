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

import com.impossibl.postgres.api.jdbc.PGType;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StructTest {

  public static class TestStruct implements SQLData {

    String str;
    String str2;
    UUID id;
    Double num;

    @Override
    public String getSQLTypeName() {
      return "teststruct";
    }

    @Override
    public void readSQL(SQLInput in, String typeName) throws SQLException {
      str = in.readString();
      str2 = in.readString();
      id = in.readObject(UUID.class);
      num = in.readObject(Double.class);
    }

    @Override
    public void writeSQL(SQLOutput out) throws SQLException {
      out.writeString(str);
      out.writeString(str2);
      out.writeObject(id, PGType.UUID);
      out.writeObject(num, JDBCType.DOUBLE);
    }

  }

  public static class TestStructArray implements SQLData {

    TestStruct[] values;

    @Override
    public String getSQLTypeName() {
      return "teststructarray";
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      values = stream.readObject(TestStruct[].class);
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeObject(values, JDBCType.ARRAY);
    }

  }

  static Connection conn;


  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createType(conn, "teststruct", "str varchar, str2 varchar, id uuid, num float");
    TestUtil.createType(conn, "teststructarray", "vals teststruct[]");
    TestUtil.createTable(conn, "struct_test", "val teststruct");
    TestUtil.createTable(conn, "struct_array_test", "val teststructarray");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(conn, "struct_test");
    TestUtil.dropType(conn, "teststruct");
    TestUtil.dropType(conn, "teststructarray");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testEmbedded() throws Exception {

    TestStruct nullts = new TestStruct();

    TestStruct ts = new TestStruct();
    ts.id = UUID.randomUUID();
    ts.num = new Random().nextDouble();
    ts.str = "A string";
    ts.str2 = "A second string";

    TestStructArray tsa = new TestStructArray(), tsa2;
    tsa.values = new TestStruct[] {ts, nullts, null};

    PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_array_test VALUES (?)");
    pst.setObject(1, tsa);
    pst.executeUpdate();
    pst.close();

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM struct_array_test;");
    assertTrue(rs.next());
    assertNotNull(tsa2 = rs.getObject(1, TestStructArray.class));
    assertNotNull(tsa2.values[0]);
    assertNotNull(tsa2.values[1]);
    assertNull(tsa2.values[2]);

    TestStruct ts0 = tsa2.values[0];
    assertEquals(ts.str, ts0.str);
    assertEquals(ts.str2, ts0.str2);
    assertEquals(ts.id, ts0.id);
    assertEquals(ts.num, ts0.num, 0.00000001);

    TestStruct ts1 = tsa2.values[1];
    assertNull(ts1.str);
    assertNull(ts1.str2);
    assertNull(ts1.id);
    assertNull(ts1.num);

    rs.close();
    st.close();
  }

  @Test
  public void testSpecificType() throws SQLException {

    TestStruct ts = new TestStruct(), ts2;
    ts.id = UUID.randomUUID();
    ts.num = new Random().nextDouble();
    ts.str = "!}({%*}{%}{(%&}{%^}{&";
    ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";

    PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
    pst.setObject(1, ts);
    pst.executeUpdate();
    pst.close();

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
    assertTrue(rs.next());
    assertNotNull(ts2 = rs.getObject(1, TestStruct.class));
    assertEquals(ts.str, ts2.str);
    assertEquals(ts.str2, ts2.str2);
    assertEquals(ts.id, ts2.id);
    assertEquals(ts.num, ts2.num, 0.00000001);
    rs.close();
    st.close();
  }

  @Test
  public void testResultSetTypeMap() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<>();
    typeMap.put("teststruct", TestStruct.class);

    TestStruct ts = new TestStruct(), ts2;
    ts.id = UUID.randomUUID();
    ts.num = new Random().nextDouble();
    ts.str = "!}({%*}{%}{(%&}{%^}{&";
    ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";

    PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
    pst.setObject(1, ts);
    pst.executeUpdate();
    pst.close();

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM struct_test");
    assertTrue(rs.next());
    assertNotNull(ts2 = (TestStruct) rs.getObject(1, typeMap));
    assertEquals(ts.str, ts2.str);
    assertEquals(ts.str2, ts2.str2);
    assertEquals(ts.id, ts2.id);
    assertEquals(ts.num, ts2.num, 0.00000001);
    rs.close();
    st.close();
  }

  @Test
  public void testConnectionTypeMap() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<>();
    typeMap.put("teststruct", TestStruct.class);

    conn.setTypeMap(typeMap);

    TestStruct ts = new TestStruct(), ts2;
    ts.id = UUID.randomUUID();
    ts.num = new Random().nextDouble();
    ts.str = "!}({%*}{%}{(%&}{%^}{&";
    ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";

    PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
    pst.setObject(1, ts);
    pst.executeUpdate();
    pst.close();

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
    assertTrue(rs.next());
    assertNotNull(ts2 = (TestStruct) rs.getObject(1, typeMap));
    assertEquals(ts.str, ts2.str);
    assertEquals(ts.str2, ts2.str2);
    assertEquals(ts.id, ts2.id);
    assertEquals(ts.num, ts2.num, 0.00000001);
    rs.close();
    st.close();
  }

  @Test
  public void testConnectionTypeMapFail() throws SQLException {

    TestStruct ts = new TestStruct();
    ts.id = UUID.randomUUID();
    ts.num = new Random().nextDouble();
    ts.str = "!}({%*}{%}{(%&}{%^}{&";
    ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";

    PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
    pst.setObject(1, ts);
    pst.executeUpdate();
    pst.close();

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
    assertTrue(rs.next());

    try {
      TestStruct ts2 = (TestStruct) rs.getObject(1);
      Assert.fail("Cast should have failed");
    }
    catch (ClassCastException e) {
      //Should fail
    }
    finally {
      rs.close();
      st.close();
    }

  }

}
