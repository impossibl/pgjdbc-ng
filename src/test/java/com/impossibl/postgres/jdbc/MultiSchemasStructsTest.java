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

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by romastar on 07.07.15.
 */
public class MultiSchemasStructsTest {

  private static final String FIRST_SCHEMA = "first";
  private static final String SECOND_SCHEMA = "second";

  private static final String TABLE_NAME = "tbl_user";
  private static final String TYPE_NAME = "t_user";

  public static class FirstSchemaUser implements SQLData {

    Integer id;
    String name;

    @Override
    public String getSQLTypeName() {
      return FIRST_SCHEMA + "." + TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput in, String typeName) throws SQLException {
      id = in.readInt();
      name = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput out) throws SQLException {
      out.writeInt(id);
      out.writeString(name);
    }

  }

  public static class SecondSchemaUser implements SQLData {

    Integer id;
    String name;
    String email;

    @Override
    public String getSQLTypeName() {
      return SECOND_SCHEMA + "." + TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput in, String typeName) throws SQLException {
      id = in.readInt();
      name = in.readString();
      email = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput out) throws SQLException {
      out.writeInt(id);
      out.writeString(name);
      out.writeString(email);
    }
  }

  public static class PublicSchemaUser implements SQLData {

    String firstName;
    String lastName;

    @Override
    public String getSQLTypeName() {
      return  TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput in, String typeName) throws SQLException {
      firstName = in.readString();
      lastName = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput out) throws SQLException {
      out.writeString(firstName);
      out.writeString(lastName);
    }

  }


  static Connection conn;


  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();

    TestUtil.createSchema(conn, FIRST_SCHEMA);
    TestUtil.createTable(conn, FIRST_SCHEMA + "." + TABLE_NAME, "id integer, name varchar(50)");
    TestUtil.createType(conn, FIRST_SCHEMA + "." + TYPE_NAME, "id integer, name varchar(50)");

    TestUtil.createSchema(conn, SECOND_SCHEMA);
    TestUtil.createTable(conn, SECOND_SCHEMA + "." + TABLE_NAME, "id integer, name varchar(50), email varchar(128)");
    TestUtil.createType(conn, SECOND_SCHEMA + "." + TYPE_NAME, "id integer, name varchar(50), email varchar(128)");

    TestUtil.createTable(conn, TABLE_NAME, "first_name varchar(50), last_name varchar(50)");
    TestUtil.createType(conn, TYPE_NAME, "first_name varchar(50), last_name varchar(50)");


    String firstPutFunctionTemplate =
        "CREATE OR REPLACE FUNCTION " + FIRST_SCHEMA + ".fn_put_user(usr " + FIRST_SCHEMA + "." + TYPE_NAME + ")"
            + " RETURNS void AS $BODY$ begin"
            + " insert into " + FIRST_SCHEMA + "." + TABLE_NAME + "(id, name) values(usr.id, usr.name);"
            + "end; $BODY$ LANGUAGE plpgsql";

    String secondPutFunctionTemplate =
        "CREATE OR REPLACE FUNCTION " + SECOND_SCHEMA + ".fn_put_user(usr " + SECOND_SCHEMA + "." + TYPE_NAME + ")"
            + " RETURNS void AS $BODY$ begin"
            + " insert into " + SECOND_SCHEMA + "." + TABLE_NAME + "(id, name, email) values(usr.id, usr.name, usr.email);"
            + "end; $BODY$ LANGUAGE plpgsql";

    String publicPutFunctionTemplate =
        "CREATE OR REPLACE FUNCTION fn_put_user(usr " + TYPE_NAME + ")"
            + " RETURNS void AS $BODY$ begin"
            + " insert into " + TABLE_NAME + "(first_name, last_name) values(usr.first_name, usr.last_name);"
            + "end; $BODY$ LANGUAGE plpgsql";


    String firstGetUsersAsArray =
        "CREATE OR REPLACE FUNCTION " + FIRST_SCHEMA + ".fn_get_users()\n" +
            "  RETURNS " + FIRST_SCHEMA + "." + TYPE_NAME + "[] AS $BODY$\n" +
            "declare\n" +
            "\tusers " + FIRST_SCHEMA + "." + TYPE_NAME + "[];\n" +
            "\tusr " + FIRST_SCHEMA + "." + TYPE_NAME + ";\n" +
            "begin\n" +
            "\tfor usr in select * from " + FIRST_SCHEMA + "." + TABLE_NAME + " loop\n" +
            "\t\tusers := array_append(users, usr);\n" +
            "\tend loop;\n" +
            "\treturn users;\n" +
            "end;\n" +
            "$BODY$ LANGUAGE plpgsql";


    String secondGetUsersAsArray =
        "CREATE OR REPLACE FUNCTION " + SECOND_SCHEMA + ".fn_get_users()\n" +
            "  RETURNS " + SECOND_SCHEMA + "." + TYPE_NAME + "[] AS $BODY$\n" +
            "declare\n" +
            "\tusers " + SECOND_SCHEMA + "." + TYPE_NAME + "[];\n" +
            "\tusr " + SECOND_SCHEMA + "." + TYPE_NAME + ";\n" +
            "begin\n" +
            "\tfor usr in select * from " + SECOND_SCHEMA + "." + TABLE_NAME + " loop\n" +
            "\t\tusers := array_append(users, usr);\n" +
            "\tend loop;\n" +
            "\treturn users;\n" +
            "end;\n" +
            "$BODY$ LANGUAGE plpgsql";

    String publicGetUsersAsArray =
        "CREATE OR REPLACE FUNCTION fn_get_users()\n" +
            "  RETURNS " + TYPE_NAME + "[] AS $BODY$\n" +
            "declare\n" +
            "\tusers " + TYPE_NAME + "[];\n" +
            "\tusr " + TYPE_NAME + ";\n" +
            "begin\n" +
            "\tfor usr in select * from " + TABLE_NAME + " loop\n" +
            "\t\tusers := array_append(users, usr);\n" +
            "\tend loop;\n" +
            "\treturn users;\n" +
            "end;\n" +
            "$BODY$ LANGUAGE plpgsql";

    Statement stmt = conn.createStatement();

    stmt.execute(firstPutFunctionTemplate);
    stmt.execute(secondPutFunctionTemplate);

    stmt.execute(firstGetUsersAsArray);
    stmt.execute(secondGetUsersAsArray);

    stmt.execute(publicPutFunctionTemplate);
    stmt.execute(publicGetUsersAsArray);
  }

  @After
  public void tearDown() throws Exception {
    dropTestObjects();
    TestUtil.closeDB(conn);
  }

  private void dropTestObjects() throws Exception {
    TestUtil.dropSchema(conn, FIRST_SCHEMA);
    TestUtil.dropSchema(conn, SECOND_SCHEMA);

    Statement stmt = conn.createStatement();
    stmt.execute("DROP FUNCTION fn_put_user(t_user);");
    stmt.execute("DROP FUNCTION fn_get_users();");

    TestUtil.dropTable(conn, TABLE_NAME);
    TestUtil.dropType(conn, TYPE_NAME);
  }

  @Test
  public void testStructCreateDynamic() throws SQLException {

    putPublicStruct();
    putFirstStruct();
    putSecondStruct();

    checkPublicStructs();
    checkFirstStructs();
    checkSecondStructs();
  }

  @Test
  public void testStructsWithoutDefault() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<>();
    typeMap.put("first.t_user", FirstSchemaUser.class);
    typeMap.put("second.t_user", SecondSchemaUser.class);

    conn.setTypeMap(typeMap);

    putFirstUser();
    putSecondUser();

    checkFirstUsers();
    checkSecondsUsers();
  }

  @Test
  public void testStructsWithDefault() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<>();
    typeMap.put("first.t_user", FirstSchemaUser.class);
    typeMap.put("second.t_user", SecondSchemaUser.class);
    typeMap.put("t_user", PublicSchemaUser.class);

    conn.setTypeMap(typeMap);

    putPublicUser();
    putFirstUser();
    putSecondUser();

    checkPublicUsers();
    checkFirstUsers();
    checkSecondsUsers();
  }

  private void putFirstStruct() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_put_user(?) ");


    Struct struct = conn.createStruct(FIRST_SCHEMA + "." + TYPE_NAME, new Object[] {1d, "First user"});
    statement.setObject(1, struct, Types.STRUCT);

    statement.execute();
    statement.close();
  }

  private void putSecondStruct() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_put_user(?) ");


    Struct struct = conn.createStruct(SECOND_SCHEMA + "." + TYPE_NAME, new Object[] {1d, "Second user", "second_user@mail.com"});
    statement.setObject(1, struct, Types.STRUCT);

    statement.execute();
    statement.close();
  }

  private void putPublicStruct() throws SQLException {
    CallableStatement statement = conn.prepareCall("select fn_put_user(?) ");


    Struct struct = conn.createStruct(TYPE_NAME, new Object[] {"First name", "Last name"});
    statement.setObject(1, struct, Types.STRUCT);

    statement.execute();
    statement.close();
  }

  private void putFirstUser() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_put_user(?) ");

    FirstSchemaUser userFirst = new FirstSchemaUser();
    userFirst.id = 1;
    userFirst.name = "First user";
    statement.setObject(1, userFirst, Types.STRUCT);

    statement.execute();
    statement.close();
  }

  private void putSecondUser() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_put_user(?) ");

    SecondSchemaUser secondUser = new SecondSchemaUser();
    secondUser.id = 1;
    secondUser.name = "Second user";
    secondUser.email = "second_user@mail.com";
    statement.setObject(1, secondUser, Types.STRUCT);

    statement.execute();
    statement.close();

  }

  private void putPublicUser() throws SQLException {

    CallableStatement statement = conn.prepareCall("select fn_put_user(?) ");

    PublicSchemaUser userPublic = new PublicSchemaUser();
    userPublic.firstName = "First name";
    userPublic.lastName = "Last name";
    statement.setObject(1, userPublic, Types.STRUCT);

    statement.execute();
    statement.close();

  }

  private Object[] getFirstUsers() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);
    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    array.free();
    statement.close();

    return (Object[]) users;
  }

  private Object[] getSecondUsers() throws SQLException {

    CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);
    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    array.free();
    statement.close();

    return (Object[]) users;
  }

  private Object[] getPublicUsers() throws SQLException {
    CallableStatement statement = conn.prepareCall("select fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);

    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    array.free();
    statement.close();

    return (Object[]) users;
  }

  private void checkFirstUsers() throws SQLException {
    try {
      FirstSchemaUser[] users = (FirstSchemaUser[]) getFirstUsers();
      assertEquals(1, users.length);
      assertEquals(1d, users[0].id, 0d);
      assertEquals("First user", users[0].name);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to FirstSchemaUser");
    }
  }

  private void checkSecondsUsers() throws SQLException {
    try {
      SecondSchemaUser[] users = (SecondSchemaUser[]) getSecondUsers();
      assertEquals(1, users.length);
      assertEquals(1d, users[0].id, 0d);
      assertEquals("Second user", users[0].name);
      assertEquals("second_user@mail.com", users[0].email);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to SecondSchemaUser");
    }
  }

  private void checkPublicUsers() throws SQLException {
    try {
      PublicSchemaUser[] publicUsers = (PublicSchemaUser[]) getPublicUsers();
      assertEquals(1, publicUsers.length);
      assertEquals("First name", publicUsers[0].firstName);
      assertEquals("Last name", publicUsers[0].lastName);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to PublicSchemaUser");
    }
  }
  private void checkFirstStructs() throws SQLException {
    try {
      Struct[] users = (Struct[]) getFirstUsers();
      assertEquals(1, users.length);
      Object[] userAttrs = users[0].getAttributes();
      assertEquals(2, userAttrs.length);
      assertEquals(1, userAttrs[0]);
      assertEquals("First user", userAttrs[1]);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to Struct");
    }
  }

  private void checkSecondStructs() throws SQLException {
    try {
      Struct[] users = (Struct[]) getSecondUsers();
      assertEquals(1, users.length);
      Object[] userAttrs = users[0].getAttributes();
      assertEquals(3, userAttrs.length);
      assertEquals(1, userAttrs[0]);
      assertEquals("Second user", userAttrs[1]);
      assertEquals("second_user@mail.com", userAttrs[2]);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to Struct");
    }
  }

  private void checkPublicStructs() throws SQLException {
    try {
      Struct[] users = (Struct[]) getPublicUsers();
      assertEquals(1, users.length);
      Object[] userAttrs = users[0].getAttributes();
      assertEquals(2, userAttrs.length);
      assertEquals("First name", userAttrs[0]);
      assertEquals("Last name", userAttrs[1]);
    }
    catch (ClassCastException e) {
      fail("Invalid casting to Struct");
    }
  }


}
