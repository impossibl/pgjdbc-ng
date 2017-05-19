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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
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

    // Ask for Updateable ResultSets
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

    // Ask for Updateable ResultSets
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
    con = TestUtil.openDB();

    String testStr = "This Is OuR TeSt message";

    // The connection must be ours!
    assertTrue(con instanceof PGConnectionImpl);

    // Clear any existing warnings
    con.clearWarnings();

    // Set the test warning
    ((PGConnectionImpl)con).addWarning(new SQLWarning(testStr));

    // Retrieve it
    SQLWarning warning = con.getWarnings();
    assertNotNull(warning);
    assertEquals(testStr, warning.getMessage());

    // Finally test clearWarnings() this time there must be something to delete
    con.clearWarnings();
    assertTrue(con.getWarnings() == null);
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

    con = TestUtil.openDB();
    con.setNetworkTimeout(null, 1000);
    con.setAutoCommit(false);

    // TODO fixme when running in CI
    if (con.getMetaData().getDatabaseMajorVersion() == 9 && con.getMetaData().getDatabaseMinorVersion() == 1) {
      return;
    }

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
  }

  /**
   * Abort connection
   */
  @Test
  public void testAbort() throws Exception {

    con = TestUtil.openDB();

    Thread queryThread = new Thread() {

      @Override
      public void run() {

        try {

          try (Statement stmt = con.createStatement()) {

            stmt.execute("SELECT pg_sleep(10);");

          }

        }
        catch (SQLException e) {
          // Ignore
        }

      }

    };

    queryThread.start();

    Executor executor = new Executor() {

      @Override
      public void execute(Runnable command) {
        command.run();
      }
    };

    long start = System.currentTimeMillis();

    con.abort(executor);

    queryThread.join();

    assertTrue(System.currentTimeMillis() - start < 10000);
    assertTrue(con.isClosed());

  }

}
