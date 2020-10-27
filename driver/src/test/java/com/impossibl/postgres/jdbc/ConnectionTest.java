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

import com.impossibl.postgres.protocol.TransactionStatus;

import static com.impossibl.postgres.jdbc.JDBCSettings.CI_APPLICATION_NAME;
import static com.impossibl.postgres.jdbc.JDBCSettings.CI_CLIENT_USER;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * TestCase to test the internal functionality of org.postgresql.jdbc2.Connection
 * and it's superclass.
 *
 */
@RunWith(JUnit4.class)
public class ConnectionTest {

  private Connection con;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();

    TestUtil.createTable(con, "test_a", "imagename name,image oid,id int4");
    TestUtil.createTable(con, "test_c", "source text,cost money,imageid int4");

    TestUtil.closeDB(con);
  }

  @After
  public void after() throws Exception {
    TestUtil.closeDB(con);

    con = TestUtil.openDB();

    TestUtil.dropTable(con, "test_a");
    TestUtil.dropTable(con, "test_c");

    TestUtil.closeDB(con);
  }

  /*
   * Tests the two forms of createStatement()
   */
  @Test
  public void testCreateStatement() throws Exception {
    con = TestUtil.openDB();

    // A standard Statement
    Statement stat = con.createStatement();
    assertNotNull(stat);
    stat.close();

    // Ask for Updatable ResultSets
    stat = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    assertNotNull(stat);
    stat.close();
  }

  /*
   * Tests the two forms of prepareStatement()
   */
  @Test
  public void testPrepareStatement() throws Exception {
    con = TestUtil.openDB();

    String sql = "select source,cost,imageid from test_c";

    // A standard Statement
    PreparedStatement stat = con.prepareStatement(sql);
    assertNotNull(stat);
    stat.close();

    // Ask for Updatable ResultSets
    stat = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    assertNotNull(stat);
    stat.close();
  }

  /*
   * Put the test for createPrepareCall here
   */
  @Ignore
  public void testPrepareCall() {
  }

  /*
   * Test nativeSQL
   */
  @Test
  public void testNativeSQL() throws Exception {
    // test a simple escape
    con = TestUtil.openDB();
    assertEquals("DATE '2005-01-24'", con.nativeSQL("{d '2005-01-24'}"));
  }

  /*
   * Test autoCommit (both get & set)
   */
  @Test
  public void testTransactions() throws Exception {
    con = TestUtil.openDB();
    Statement st;
    ResultSet rs;

    // Turn it off
    con.setAutoCommit(false);
    assertTrue(!con.getAutoCommit());

    // Turn it back on
    con.setAutoCommit(true);
    assertTrue(con.getAutoCommit());

    // Now test commit
    st = con.createStatement();
    st.executeUpdate("insert into test_a (imagename,image,id) values ('comttest',1234,5678)");

    con.setAutoCommit(false);

    // Now update image to 9876 and commit
    st.executeUpdate("update test_a set image=9876 where id=5678");
    con.commit();
    rs = st.executeQuery("select image from test_a where id=5678");
    assertTrue(rs.next());
    assertEquals(9876, rs.getInt(1));
    rs.close();

    // Now try to change it but rollback
    st.executeUpdate("update test_a set image=1111 where id=5678");
    con.rollback();
    rs = st.executeQuery("select image from test_a where id=5678");
    assertTrue(rs.next());
    assertEquals(9876, rs.getInt(1)); // Should not change!
    rs.close();
    st.close();
  }

  /*
   * Simple test to see if isClosed works.
   */
  @Test
  public void testIsClosed() throws Exception {
    con = TestUtil.openDB();

    // Should not say closed
    assertTrue(!con.isClosed());

    TestUtil.closeDB(con);

    // Should now say closed
    assertTrue(con.isClosed());
  }

  /*
   * Test the warnings system
   */
  @Test
  public void testWarnings() throws Exception {
    con = TestUtil.openDB().unwrap(PGDirectConnection.class);
    // The connection must be ours!
    assertTrue(con != null);

    String testStr = "This Is OuR TeSt message";

    // Clear any existing warnings
    con.clearWarnings();

    // Set the test warning
    ((PGDirectConnection)con).addWarning(new SQLWarning(testStr));

    // Retrieve it
    SQLWarning warning = con.getWarnings();
    assertNotNull(warning);
    assertEquals(testStr, warning.getMessage());

    // Finally test clearWarnings() this time there must be something to delete
    con.clearWarnings();
    assertNull(con.getWarnings());
  }

  /*
   * Transaction Isolation Levels
   */
  @Test
  public void testTransactionIsolation() throws Exception {
    con = TestUtil.openDB();

    int defaultLevel = con.getTransactionIsolation();

    // Begin a transaction
    con.setAutoCommit(false);

    // The isolation level should not have changed
    assertEquals(defaultLevel, con.getTransactionIsolation());

    // Now run some tests with autocommit enabled.
    con.setAutoCommit(true);

    assertEquals(defaultLevel, con.getTransactionIsolation());

    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    assertEquals(Connection.TRANSACTION_SERIALIZABLE,
        con.getTransactionIsolation());

    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());

    // Test if a change of isolation level before beginning the
    // transaction affects the isolation level inside the transaction.
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    assertEquals(Connection.TRANSACTION_SERIALIZABLE,
        con.getTransactionIsolation());
    con.setAutoCommit(false);
    assertEquals(Connection.TRANSACTION_SERIALIZABLE,
        con.getTransactionIsolation());
    con.setAutoCommit(true);
    assertEquals(Connection.TRANSACTION_SERIALIZABLE,
        con.getTransactionIsolation());
    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED,
        con.getTransactionIsolation());
    con.setAutoCommit(false);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED,
        con.getTransactionIsolation());
    con.commit();

    // Test that getTransactionIsolation() does not actually start a new txn.
    con.getTransactionIsolation(); // Shouldn't start a new transaction.
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); // Should be ok -- we're not in a transaction.
    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); // Should still be ok.

    // Test that we can't change isolation mid-transaction
    //TODO: reconcile against mainstream driver
    //        Statement stmt = con.createStatement();
    //        stmt.executeQuery("SELECT 1");          // Start transaction.
    //        stmt.close();
    //
    //        try
    //        {
    //            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    //            fail("Expected an exception when changing transaction isolation mid-transaction");
    //        }
    //        catch (SQLException e)
    //        {
    //            // Ok.
    //        }

    con.rollback();
  }

  /*
   * JDBC2 Type mappings
   */
  @Test
  public void testTypeMaps() throws Exception {
    con = TestUtil.openDB();

    // preserve the current map
    Map<String, Class<?>> oldmap = con.getTypeMap();

    // now change it for an empty one
    Map<String, Class<?>> newmap = new HashMap<>();
    con.setTypeMap(newmap);
    assertEquals(newmap, con.getTypeMap());

    // restore the old one
    con.setTypeMap(oldmap);
    assertEquals(oldmap, con.getTypeMap());

  }

  /**
   * Closing a Connection more than once is not an error.
   */
  @Test
  public void testDoubleClose() throws Exception {
    con = TestUtil.openDB();
    con.close();
    con.close();
  }

  /**
   * Network timeout enforcement
   */
  @Test
  public void testNetworkTimeout() throws Exception {

    con = TestUtil.openDB();

    assertEquals(0, con.getNetworkTimeout());
    con.setNetworkTimeout(null, 1000);
    assertEquals(1000, con.getNetworkTimeout());

    Statement stmt = con.createStatement();
    try {
      stmt.execute("SELECT pg_sleep(10);");
      fail("Expected SQLTimeoutException");
    }
    catch (SQLTimeoutException e) {
      // Ok
    }
    catch (Throwable t) {
      throw t;
    }
    finally {
      stmt.close();
    }

    assertTrue(con.isClosed());
  }

  /**
   * Kill connection
   */
  @Test
  public void testKillConnection() throws Exception {

    int networkTimeout = 30000;
    Connection con = TestUtil.openDB();
    con.setNetworkTimeout(null, networkTimeout + 20);
    con.setAutoCommit(false);

    long pid = -1;
    try (PreparedStatement ps = con.prepareStatement("SELECT pg_backend_pid()")) {
      ResultSet rs = ps.executeQuery();
      rs.next();
      pid = rs.getLong(1);
      rs.close();
    }
    con.commit();

    Statement stmt = con.createStatement();

    // Get a new connection and kill the first one
    Connection killer = TestUtil.openDB();
    try (Statement kstmt = killer.createStatement()) {
      kstmt.execute("SELECT pg_terminate_backend(" + pid + ")");
    }
    killer.close();

    Thread.sleep(200);

    long start = System.currentTimeMillis();

    try {
      stmt.execute("SELECT 1");
      fail("Expected SQLException");
    }
    catch (SQLException e) {
      // Ok
    }
    finally {
      try {
        stmt.close();
      }
      catch (SQLException ignore) {
        // Ignore
      }
    }
    assertFalse(con.isValid(5));
    assertTrue(con.isClosed());
    assertTrue("Connection was probably closed by network timeout", (System.currentTimeMillis() - start) < networkTimeout);
  }

  /**
   * Abort connection - original abort test
   * where the {@link PGDirectConnection#abort(Executor)}
   * executes before the thread, and therefore query,
   * begin executing.
   */
  @Test
  public void testAbort() throws Exception {

    con = TestUtil.openDB();
    con.setNetworkTimeout(null, 40000);

    Thread queryThread = new Thread() {

      @Override
      public void run() {

        try {

          try (Statement stmt = con.createStatement()) {

            stmt.execute("SELECT pg_sleep(30);");
            fail("Query should have been aborted");
          }

        }
        catch (SQLException e) {
        }

      }

    };

    queryThread.start();

    long start = System.currentTimeMillis();

    con.abort(Runnable::run);

    queryThread.join();

    assertTrue(System.currentTimeMillis() - start < 30000);
    assertTrue(con.isClosed());
  }

  /**
   * Abort connection - same test as above except
   * a sleep is added to ensure the query begins
   * execution before the abort is issued
   */
  @Test
  public void testAbortAfterQueryStart() throws Exception {

    con = TestUtil.openDB();
    con.setNetworkTimeout(null, 40000);

    Thread queryThread = new Thread() {

      @Override
      public void run() {

        try {

          try (Statement stmt = con.createStatement()) {

            stmt.execute("SELECT pg_sleep(30);");
            fail("Query should have been aborted");
          }

        }
        catch (SQLException e) {
        }

      }

    };

    queryThread.start();

    // Wait for query to start...
    Thread.sleep(100);

    long start = System.currentTimeMillis();

    con.abort(Runnable::run);

    queryThread.join();

    assertTrue(System.currentTimeMillis() - start < 30000);
    assertTrue(con.isClosed());
  }

  @Test
  public void testIsValidTransactionSideEffect() throws Exception {

    try (PGDirectConnection con = TestUtil.openDB().unwrap(PGDirectConnection.class)) {

      con.setAutoCommit(true);
      assertEquals(con.getTransactionStatus(), TransactionStatus.Idle);
      assertTrue(con.isValid(5));
      assertEquals(con.getTransactionStatus(), TransactionStatus.Idle);

      con.setAutoCommit(false);
      assertEquals(con.getTransactionStatus(), TransactionStatus.Idle);
      assertTrue(con.isValid(5));
      assertEquals(con.getTransactionStatus(), TransactionStatus.Idle);

      con.execute("BEGIN");

      assertEquals(con.getTransactionStatus(), TransactionStatus.Active);
      assertTrue(con.isValid(5));
      assertEquals(con.getTransactionStatus(), TransactionStatus.Active);

    }
  }

  @Test
  public void testClientInfo() throws Exception {

    con = TestUtil.openDB();

    // Test ApplicationNAme

    con.setClientInfo(CI_APPLICATION_NAME.getName(), "PGJDBC-NG Tests");
    assertEquals("PGJDBC-NG Tests", con.getClientInfo(CI_APPLICATION_NAME.getName()));

    // Test ClientUser
    try (Statement statement = con.createStatement()) {
      String testRole = "test" + Integer.toHexString(new Random().nextInt());
      statement.execute("CREATE ROLE " + testRole);

      con.setClientInfo(CI_CLIENT_USER.getName(), testRole);
      assertEquals(testRole, con.getClientInfo(CI_CLIENT_USER.getName()));
    }


  }

  @Test
  public void testSchema() throws Exception {

    con = TestUtil.openDB();

    assertEquals(con.getSchema(), "public");

    con.setSchema(null);
    con.setSchema("public");

    assertEquals(con.getSchema(), "public");
  }

  @Test
  public void testScramSha256() throws Exception {

    con = TestUtil.openDB();

    String role = "test" + Math.abs(new Random().nextInt());

    try (Statement statement = con.createStatement()) {
      statement.execute("CREATE ROLE " + role + " WITH LOGIN PASSWORD 'SCRAM-SHA-256$4096:Fgh8JU2AlRjBHUsIU/GgtQ==$XiT346dvVvPmnmTWeW0djrcMYBGuiQDh8QYbBJaBm/I=:CY9vUvDF8v6FIR8Zwircvd82YV58J5AwWiMWwfssuwg='");
    }

    try {

      Properties props = new Properties();
      props.setProperty("user", role);
      props.setProperty("password", "test");

      try (Connection roleCon = DriverManager.getConnection(TestUtil.getURL(), props)) {
        try (Statement statement = roleCon.createStatement()) {
          assertTrue(statement.execute("SELECT 1"));
        }
      }

    }
    finally {
      try (Statement statement = con.createStatement()) {
        statement.execute("DROP ROLE " + role);
      }
    }

  }

  @Test
  public void testScramSha256Plus() throws Exception {

    con = TestUtil.openDB();

    String role = "test" + Math.abs(new Random().nextInt());

    try (Statement statement = con.createStatement()) {
      statement.execute("CREATE ROLE " + role + " WITH LOGIN PASSWORD 'SCRAM-SHA-256$4096:Fgh8JU2AlRjBHUsIU/GgtQ==$XiT346dvVvPmnmTWeW0djrcMYBGuiQDh8QYbBJaBm/I=:CY9vUvDF8v6FIR8Zwircvd82YV58J5AwWiMWwfssuwg='");
    }

    try {

      Properties props = new Properties();
      props.setProperty("user", role);
      props.setProperty("password", "test");

      try (Connection roleCon = DriverManager.getConnection(TestUtil.getURL("sslMode", "require"), props)) {
        try (Statement statement = roleCon.createStatement()) {
          assertTrue(statement.execute("SELECT 1"));
        }
      }

    }
    finally {
      try (Statement statement = con.createStatement()) {
        statement.execute("DROP ROLE " + role);
      }
    }

  }

}
