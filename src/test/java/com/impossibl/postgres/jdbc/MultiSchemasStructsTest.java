package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.api.jdbc.PGSQLInput;
import com.impossibl.postgres.api.jdbc.PGSQLOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

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
    public void testStructsWithoutDefault() throws SQLException {

        Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
        typeMap.put("first.t_user", FirstSchemaUser.class);
        typeMap.put("second.t_user", SecondSchemaUser.class);

        conn.setTypeMap(typeMap);

        putFirstUser();
        putSecondUser();

        FirstSchemaUser[] firstUsers = getFirstUsers();
        SecondSchemaUser[] secondUsers = getSecondUsers();

    }

    @Test
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
        FirstSchemaUser[] users = null;
        CallableStatement statement = conn.prepareCall("select " + FIRST_SCHEMA + ".fn_get_users(?) ");

        statement.registerOutParameter(1, Types.ARRAY);
        statement.execute();

        Array array = statement.getArray(1);
        users = (FirstSchemaUser[]) array.getArray();
        statement.close();

        return users;
    }

    public SecondSchemaUser[] getSecondUsers() throws SQLException {
        SecondSchemaUser[] users = null;
        CallableStatement statement = conn.prepareCall("select " + SECOND_SCHEMA + ".fn_get_users(?) ");

        statement.registerOutParameter(1, Types.ARRAY);
        statement.execute();

        Array array = statement.getArray(1);
        users = (SecondSchemaUser[]) array.getArray(); // Working with type without namespace
        statement.close();
        return users;
    }

    public PublicSchemaUser[] getPublicUsers() throws SQLException {
        PublicSchemaUser[] users = null;
        CallableStatement statement = conn.prepareCall("select fn_get_users(?) ");

        statement.registerOutParameter(1, Types.ARRAY);

        statement.execute();

        Array array = statement.getArray(1);
        users = (PublicSchemaUser[]) array.getArray(); // Working with type without namespace
        statement.close();

        return users;
    }
}
