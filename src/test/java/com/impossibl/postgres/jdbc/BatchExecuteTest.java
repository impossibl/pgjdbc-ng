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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/* TODO tests that can be added to this test case
 * - SQLExceptions chained to a BatchUpdateException
 * - test PreparedStatement as thoroughly as Statement
 */

/*
 * Test case for Statement.batchExecute()
 */
@RunWith(JUnit4.class)
public class BatchExecuteTest {

  private Connection con;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();

    // Drop the test table if it already exists for some reason. It is
    // not an error if it doesn't exist.
    TestUtil.createTable(con, "testbatch", "pk INTEGER, col1 INTEGER");

    Statement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO testbatch VALUES (1, 0)");
    stmt.close();

    // Generally recommended with batch updates. By default we run all
    // tests in this test case with autoCommit disabled.
    con.setAutoCommit(false);
  }

  @After
  public void after() throws Exception {

    con.setAutoCommit(true);

    TestUtil.dropTable(con, "testbatch");
    TestUtil.closeDB(con);
  }

  @Test
  public void testSupportsBatchUpdates() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();
    assertTrue(dbmd.supportsBatchUpdates());
  }

  @Test
  public void testEmptyClearBatch() throws Exception {
    Statement stmt = con.createStatement();
    stmt.clearBatch(); // No-op.
    stmt.close();

    PreparedStatement ps = con.prepareStatement("SELECT ?");
    ps.clearBatch(); // No-op.
    ps.close();
  }

  private void assertCol1HasValue(int expected) throws Exception {
    Statement getCol1 = con.createStatement();

    ResultSet rs = getCol1.executeQuery("SELECT col1 FROM testbatch WHERE pk = 1");
    assertTrue(rs.next());

    int actual = rs.getInt("col1");

    assertEquals(expected, actual);

    assertEquals(false, rs.next());

    rs.close();
    getCol1.close();
  }

  @Test
  public void testExecuteEmptyBatch() throws Exception {
    Statement stmt = con.createStatement();
    int[] updateCount = stmt.executeBatch();
    assertEquals(0, updateCount.length);

    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 1 WHERE pk = 1");
    stmt.clearBatch();
    updateCount = stmt.executeBatch();
    assertEquals(0, updateCount.length);
    stmt.close();
  }

  @Test
  public void testClearBatch() throws Exception {
    Statement stmt = con.createStatement();

    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 1 WHERE pk = 1");
    assertCol1HasValue(0);
    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 2 WHERE pk = 1");
    assertCol1HasValue(0);
    stmt.clearBatch();
    assertCol1HasValue(0);
    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 4 WHERE pk = 1");
    assertCol1HasValue(0);
    stmt.executeBatch();
    assertCol1HasValue(4);
    con.commit();
    assertCol1HasValue(4);

    stmt.close();
  }

  @Test
  public void testSelectThrowsException() throws Exception {
    Statement stmt = con.createStatement();

    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 1 WHERE pk = 1");
    stmt.addBatch("SELECT col1 FROM testbatch WHERE pk = 1");
    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 2 WHERE pk = 1");

    try {
      stmt.executeBatch();
      fail("Should raise a BatchUpdateException because of the SELECT");
    }
    catch (BatchUpdateException e) {
      int[] updateCounts = e.getUpdateCounts();
      assertEquals(1, updateCounts.length);
      assertEquals(1, updateCounts[0]);
    }
    catch (SQLException e) {
      fail("Should throw a BatchUpdateException instead of " + "a generic SQLException: " + e);
    }

    stmt.close();
  }

  @Test
  public void testStringAddBatchOnPreparedStatement() throws Exception {
    PreparedStatement pstmt = con.prepareStatement("UPDATE testbatch SET col1 = col1 + ? WHERE PK = ?");
    pstmt.setInt(1, 1);
    pstmt.setInt(2, 1);
    pstmt.addBatch();

    try {
      pstmt.addBatch("UPDATE testbatch SET col1 = 3");
      fail("Should have thrown an exception about using the string addBatch method on a prepared statement.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    pstmt.close();
  }

  @Test
  public void testPreparedStatement() throws Exception {
    PreparedStatement pstmt = con.prepareStatement("UPDATE testbatch SET col1 = col1 + ? WHERE PK = ?");

    // Note that the first parameter changes for every statement in the
    // batch, whereas the second parameter remains constant.
    pstmt.setInt(1, 1);
    pstmt.setInt(2, 1);
    pstmt.addBatch();
    assertCol1HasValue(0);

    pstmt.setInt(1, 2);
    pstmt.addBatch();
    assertCol1HasValue(0);

    pstmt.setInt(1, 4);
    pstmt.addBatch();
    assertCol1HasValue(0);

    pstmt.executeBatch();
    assertCol1HasValue(7);

    // now test to see that we can still use the statement after the execute
    pstmt.setInt(1, 3);
    pstmt.addBatch();
    assertCol1HasValue(7);

    pstmt.executeBatch();
    assertCol1HasValue(10);

    con.commit();
    assertCol1HasValue(10);

    con.rollback();
    assertCol1HasValue(10);

    pstmt.close();
  }

  @Test
  public void testTransactionalBehaviour() throws Exception {
    Statement stmt = con.createStatement();

    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 1 WHERE pk = 1");
    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 2 WHERE pk = 1");
    stmt.executeBatch();
    con.rollback();
    assertCol1HasValue(0);

    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 4 WHERE pk = 1");
    stmt.addBatch("UPDATE testbatch SET col1 = col1 + 8 WHERE pk = 1");

    // The statement has been added to the batch, but it should not yet
    // have been executed.
    assertCol1HasValue(0);

    int[] updateCounts = stmt.executeBatch();
    assertEquals(2, updateCounts.length);
    assertEquals(1, updateCounts[0]);
    assertEquals(1, updateCounts[1]);

    assertCol1HasValue(12);
    con.commit();
    assertCol1HasValue(12);
    con.rollback();
    assertCol1HasValue(12);

    stmt.close();
  }

  @Test
  public void testWarningsAreCleared() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.addBatch("CREATE TEMP TABLE unused (a int primary key)");
    stmt.executeBatch();
    // Execute an empty batch to clear warnings.
    stmt.executeBatch();
    assertNull(stmt.getWarnings());
    stmt.close();
  }

  @Test
  public void testBatchEscapeProcessing() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("CREATE TEMP TABLE batchescape (d date)");

    stmt.addBatch("INSERT INTO batchescape (d) VALUES ({d '2007-11-20'})");
    stmt.executeBatch();

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO batchescape (d) VALUES ({d '2007-11-20'})");
    pstmt.addBatch();
    pstmt.executeBatch();
    pstmt.close();

    ResultSet rs = stmt.executeQuery("SELECT d FROM batchescape");
    assertTrue(rs.next());
    assertEquals("2007-11-20", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("2007-11-20", rs.getString(1));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testBatchWithEmbeddedNulls() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("CREATE TEMP TABLE batchstring (a text)");

    con.commit();

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO batchstring VALUES (?)");

    try {
      pstmt.setString(1, "a");
      pstmt.addBatch();
      pstmt.setString(1, "\u0000");
      pstmt.addBatch();
      pstmt.setString(1, "b");
      pstmt.addBatch();
      pstmt.executeBatch();
      fail("Should have thrown an exception.");
    }
    catch (SQLException sqle) {
      con.rollback();
    }
    pstmt.close();

    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batchstring");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
    rs.close();
    stmt.close();
  }

  @Test
  public void testPreparedStatementWithObject() throws SQLException {

    try (Statement stmt = con.createStatement()) {

      stmt.execute("CREATE TEMP TABLE testinet (ip inet)");

      con.commit();

      try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO testinet VALUES (?)")) {

        pstmt.setObject(1, "192.168.1.1");
        pstmt.addBatch();
        pstmt.execute();

        con.commit();
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM testinet")) {

        assertTrue(rs.next());
        assertEquals("192.168.1.1", rs.getString(1));
      }

    }

  }

}
