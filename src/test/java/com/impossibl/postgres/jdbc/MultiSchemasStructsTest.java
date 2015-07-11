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

import com.impossibl.postgres.api.jdbc.PGSQLInput;
import com.impossibl.postgres.api.jdbc.PGSQLOutput;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Created by romastar on 07.07.15.
 */
public class MultiSchemasStructsTest {

  static final String FIRST_SCHEMA = "first";
  static final String SECOND_SCHEMA = "second";

  static final String TABLE_NAME = "tbl_user";
  static final String TYPE_NAME = "t_user";

  static class FirstSchemaUser implements SQLData {

    Double id;
    String name;

    @Override
    public String getSQLTypeName() throws SQLException {
      return FIRST_SCHEMA + "." + TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      PGSQLInput in = (PGSQLInput) stream;
      id = in.readDouble();
      name = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      PGSQLOutput out = (PGSQLOutput) stream;
      out.writeDouble(id);
      out.writeString(name);
    }

  }

  static class SecondSchemaUser implements SQLData {

    Double id;
    String name;
    String email;

    @Override
    public String getSQLTypeName() throws SQLException {
      return SECOND_SCHEMA + "." + TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      PGSQLInput in = (PGSQLInput) stream;
      id = in.readDouble();
      name = in.readString();
      email = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      PGSQLOutput out = (PGSQLOutput) stream;
      out.writeDouble(id);
      out.writeString(name);
      out.writeString(email);
    }
  }

  static class PublicSchemaUser implements SQLData {

    String firstName;
    String lastName;

    @Override
    public String getSQLTypeName() throws SQLException {
      return SECOND_SCHEMA + "." + TYPE_NAME;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      PGSQLInput in = (PGSQLInput) stream;
      firstName = in.readString();
      lastName = in.readString();
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      PGSQLOutput out = (PGSQLOutput) stream;
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


    conn = TestUtil.openDB();

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

  protected void dropTestObjects() throws Exception {
    TestUtil.dropSchema(conn, FIRST_SCHEMA);
    TestUtil.dropSchema(conn, SECOND_SCHEMA);

    Statement stmt = conn.createStatement();
    stmt.execute("DROP FUNCTION fn_put_user(t_user);");
    stmt.execute("DROP FUNCTION fn_get_users();");

    TestUtil.dropTable(conn, TABLE_NAME);
    TestUtil.dropType(conn, TYPE_NAME);
  }

  @Test
  @Ignore
  public void testStructsWithoutDefault() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    typeMap.put("first.t_user", FirstSchemaUser.class);
    typeMap.put("second.t_user", SecondSchemaUser.class);

    conn.setTypeMap(typeMap);

    putFirstUser();
    putSecondUser();

    FirstSchemaUser[] firstUsers = getFirstUsers();
    SecondSchemaUser[] secondUsers = getSecondUsers();

    assertNotNull("Invalid casting to FirstSchemaUser", firstUsers);
    assertNotNull("Invalid casting to SecondSchemaUser", secondUsers);
  }

  @Test
  @Ignore
  public void testStructsWithDefault() throws SQLException {

    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    typeMap.put("first.t_user", FirstSchemaUser.class);
    typeMap.put("second.t_user", SecondSchemaUser.class);
    typeMap.put("t_user", PublicSchemaUser.class);

    conn.setTypeMap(typeMap);

    putPublicUser();
    putFirstUser();
    putSecondUser();

    PublicSchemaUser[] publicUsers = getPublicUsers();
    FirstSchemaUser[] firstUsers = getFirstUsers();
    SecondSchemaUser[] secondUsers = getSecondUsers();

    assertNotNull("Invalid casting to PublicSchemaUser", publicUsers);
    assertNotNull("Invalid casting to FirstSchemaUser", firstUsers);
    assertNotNull("Invalid casting to SecondSchemaUser", secondUsers);
  }

  public void putFirstUser() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_put_user(?) ");

    FirstSchemaUser userFirst = new FirstSchemaUser();
    userFirst.id = 1d;
    userFirst.name = "First user";
    statement.setObject(1, userFirst, Types.STRUCT);

    statement.execute();
    statement.close();
  }

  public void putSecondUser() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_put_user(?) ");

    SecondSchemaUser secondUser = new SecondSchemaUser();
    secondUser.id = 1d;
    secondUser.name = "Second user";
    secondUser.email = "second_user@mail.com";
    statement.setObject(1, secondUser, Types.STRUCT);

    statement.execute();
    statement.close();

  }

  public void putPublicUser() throws SQLException {

    CallableStatement statement = conn.prepareCall("select fn_put_user(?) ");

    PublicSchemaUser userPublic = new PublicSchemaUser();
    userPublic.firstName = "First name";
    userPublic.lastName = "Last name";
    statement.setObject(1, userPublic, Types.STRUCT);

    statement.execute();
    statement.close();

  }

  public FirstSchemaUser[] getFirstUsers() throws SQLException {
    CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);
    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    statement.close();

    if (users instanceof FirstSchemaUser[]) {
      return (FirstSchemaUser[])users;
    }

    return null;
  }

  public SecondSchemaUser[] getSecondUsers() throws SQLException {

    CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);
    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    statement.close();

    if (users instanceof SecondSchemaUser[]) {
      return (SecondSchemaUser[])users;
    }

    return null;
  }

  public PublicSchemaUser[] getPublicUsers() throws SQLException {
    CallableStatement statement = conn.prepareCall("select fn_get_users(?) ");

    statement.registerOutParameter(1, Types.ARRAY);

    statement.execute();

    Array array = statement.getArray(1);
    Object users = array.getArray();
    statement.close();

    if (users instanceof PublicSchemaUser[]) {
      return (PublicSchemaUser[])users;
    }

    return null;
  }
}
