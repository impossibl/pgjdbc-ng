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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
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

public class StructTest {

  static class TestStruct implements SQLData {

    String str;
    String str2;
    UUID id;
    Double num;

    @Override
    public String getSQLTypeName() throws SQLException {
      return "teststruct";
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      str = stream.readString();
      str2 = stream.readString();
      id = (UUID) stream.readObject();
      num = stream.readDouble();
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeString(str);
      stream.writeString(str2);
      ((PGSQLOutput)stream).writeObject(id);
      stream.writeDouble(num);
    }

  }

  static Connection conn;


  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createType(conn, "teststruct" , "str text, str2 text, id uuid, num float");
    TestUtil.createTable(conn, "struct_test", "val teststruct");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(conn, "struct_test");
    TestUtil.dropType(conn, "teststruct");
    TestUtil.closeDB(conn);
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
  }

  @Test
  public void testResultSetTypeMap() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
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
    ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
    assertTrue(rs.next());
    assertNotNull(ts2 = (TestStruct) rs.getObject(1, typeMap));
    assertEquals(ts.str, ts2.str);
    assertEquals(ts.str2, ts2.str2);
    assertEquals(ts.id, ts2.id);
    assertEquals(ts.num, ts2.num, 0.00000001);

  }

  @Test
  public void testConnectionTypeMap() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
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

  }

  @Test
  public void testConnectionTypeMapFail() throws SQLException {

    @SuppressWarnings("unused")
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

    try {
      ts2 = (TestStruct) rs.getObject(1);
      Assert.fail("Cast should have failed");
    }
    catch(ClassCastException e) {
      //Should fail
    }
    finally {
      rs.close();
      st.close();
    }

  }

}
