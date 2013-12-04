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

import com.impossibl.postgres.utils.guava.Joiner;

import static com.impossibl.postgres.utils.guava.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.XAConnection;

public class TestUtil {

  public static String getURL(Object... urlParams) {

    String query = "";
    if (urlParams != null && urlParams.length > 0) {
      query = "?" + Joiner.on("&").withKeyValueSeparator("=").join(params(urlParams));
    }

    if (!"5432".equals(getPort())) {
      return "jdbc:pgsql://" + getServer() + ":" + getPort() + "/" + getDatabase() + query;
    }
    else {
      return "jdbc:pgsql://" + getServer() + "/" + getDatabase() + query;
    }
  }

  public static String getServer() {
    return System.getProperty("pgjdbc.test.server", "localhost");
  }

  public static String getPort() {
    return System.getProperty("pgjdbc.test.port", "5432");
  }

  public static String getDatabase() {
    return System.getProperty("pgjdbc.test.db", "test");
  }

  public static Properties getProperties() {

    Properties props = new Properties();

    props.setProperty("user", getUser());
    props.setProperty("password", getPassword());

    return props;
  }

  public static String getUser() {
    return System.getProperty("pgjdbc.test.user", "pgjdbc");
  }

  public static String getPassword() {
    return System.getProperty("pgjdbc.test.password", "test");
  }

  private static Map<String, Object> params(Object... objs) {

    checkArgument(objs.length % 2 == 0);

    Map<String, Object> map = new HashMap<>();
    for (int c = 0; c < objs.length; c += 2)
      map.put((String) objs[c], objs[c + 1]);

    return map;
  }

  /*
   * Helper - opens a connection.
   */
  public static Connection openDB() throws Exception {
    return openDB(new Properties());
  }

  /*
   * Helper - opens a connection with the allowance for passing additional
   * parameters, like "compatible".
   */
  public static Connection openDB(Properties props) throws Exception {

    props.setProperty("user", getUser());
    props.setProperty("password", getPassword());

    return DriverManager.getConnection(getURL(), props);
  }

  /*
   * Helper - closes an open connection.
   */
  public static void closeDB(Connection con) throws SQLException {
    if (con != null)
      con.close();
  }

  /*
   * Helper - closes an open connection.
   */
  public static void closeDB(XAConnection con) throws SQLException {
    if (con != null)
      con.close();
  }

  /*
   * Helper - creates a test table for use by a test
   */
  public static void createTable(Connection con, String table, String columns) throws SQLException {
    // by default we don't request oids.
    createTable(con, table, columns, false);
  }

  /*
   * Helper - creates a test table for use by a test
   */
  public static void createTable(Connection con, String table, String columns, boolean withOids) throws SQLException {
    Statement st = con.createStatement();
    try {
      // Drop the table
      dropTable(con, table);

      // Now create the table
      String sql = "CREATE TABLE " + table + " (" + columns + ") ";

      if (withOids) {
        sql += " WITH OIDS";
      }
      st.executeUpdate(sql);
    }
    finally {
      st.close();
    }
  }

  /*
   * Helper - creates a test type for use by a test
   */
  public static void createType(Connection con, String type, String attrs) throws SQLException {
    Statement st = con.createStatement();
    try {
      // Drop the table
      dropType(con, type);

      // Now create the table
      String sql = "CREATE TYPE " + type + " AS (" + attrs + ") ";

      st.executeUpdate(sql);
    }
    finally {
      st.close();
    }
  }

  /**
   * Helper creates a temporary table
   *
   * @param con
   *          Connection
   * @param table
   *          String
   * @param columns
   *          String
   * @throws SQLException
   */

  public static void createTempTable(Connection con, String table, String columns) throws SQLException {
    Statement st = con.createStatement();
    try {
      // Drop the table
      dropTable(con, table);

      // Now create the table
      st.executeUpdate("create temp table " + table + " (" + columns + ")");
    }
    finally {
      st.close();
    }
  }

  /*
   * drop a sequence because older versions don't have dependency information
   * for serials
   */
  public static void dropSequence(Connection con, String sequence) throws SQLException {
    Statement stmt = con.createStatement();
    try {
      String sql = "DROP SEQUENCE " + sequence;
      stmt.executeUpdate(sql);
    }
    catch (SQLException sqle) {
      if (!con.getAutoCommit())
        throw sqle;
    }
    finally {
      stmt.close();
    }
  }

  /*
   * Helper - drops a table
   */
  public static void dropTable(Connection con, String table) throws SQLException {
    Statement stmt = con.createStatement();
    try {
      String sql = "DROP TABLE " + table + " CASCADE ";
      stmt.executeUpdate(sql);
    }
    catch (SQLException ex) {
      // Since every create table issues a drop table
      // it's easy to get a table doesn't exist error.
      // we want to ignore these, but if we're in a
      // transaction then we've got trouble
      if (!con.getAutoCommit())
        throw ex;
    }
    finally {
      stmt.close();
    }
  }

  /*
   * Helper - drops a type
   */
  public static void dropType(Connection con, String type) throws SQLException {
    Statement stmt = con.createStatement();
    try {
      String sql = "DROP TYPE " + type + " CASCADE ";
      stmt.executeUpdate(sql);
    }
    catch (SQLException ex) {
      // Since every create table issues a drop table
      // it's easy to get a table doesn't exist error.
      // we want to ignore these, but if we're in a
      // transaction then we've got trouble
      if (!con.getAutoCommit())
        throw ex;
    }
    finally {
      stmt.close();
    }
  }

  /*
   * Helper - generates INSERT SQL - very simple
   */
  public static String insertSQL(String table, String values) {
    return insertSQL(table, null, values);
  }

  public static String insertSQL(String table, String columns, String values) {
    String s = "INSERT INTO " + table;

    if (columns != null)
      s = s + " (" + columns + ")";

    return s + " VALUES (" + values + ")";
  }

  /*
   * Helper - generates SELECT SQL - very simple
   */
  public static String selectSQL(String table, String columns) {
    return selectSQL(table, columns, null, null);
  }

  public static String selectSQL(String table, String columns, String where) {
    return selectSQL(table, columns, where, null);
  }

  public static String selectSQL(String table, String columns, String where, String other) {
    String s = "SELECT " + columns + " FROM " + table;

    if (where != null)
      s = s + " WHERE " + where;
    if (other != null)
      s = s + " " + other;

    return s;
  }

  /**
   * Print a ResultSet to System.out. This is useful for debugging tests.
   */
  public static void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      if (i != 1) {
        System.out.print(", ");
      }
      System.out.print(rsmd.getColumnName(i));
    }
    System.out.println();
    while (rs.next()) {
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        if (i != 1) {
          System.out.print(", ");
        }
        System.out.print(rs.getString(i));
      }
      System.out.println();
    }
  }

  public static boolean getStandardConformingStrings(Connection con) throws SQLException {

    Statement stmt = con.createStatement();
    stmt.closeOnCompletion();

    ResultSet rs = stmt.executeQuery("SHOW standard_conforming_strings");
    if (rs.next()) {
      return rs.getBoolean(1);
    }

    return false;
  }

  public static String fix(int v, int l) {
    String s = "0000000000".substring(0, l) + Integer.toString(v);
    return s.substring(s.length() - l);
  }

  public static boolean isExtensionInstalled(Connection conn, String extensionName) throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM pg_extension WHERE extname = '" + extensionName + "'")) {

        return rs.next();

      }

    }

  }

}
