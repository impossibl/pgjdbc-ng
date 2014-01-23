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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



/*
 *  Tests for using non-zero setFetchSize().
 */
@RunWith(JUnit4.class)
public class CursorFetchTest {

  private Connection con;

  @Before
  public void setUp() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "test_fetch", "value integer");
    con.setAutoCommit(false);
  }

  @After
  public void tearDown() throws Exception {
    if (!con.getAutoCommit())
      con.rollback();

    con.setAutoCommit(true);
    TestUtil.dropTable(con, "test_fetch");
    TestUtil.closeDB(con);
  }

  protected void createRows(int count) throws Exception {
    PreparedStatement stmt = con.prepareStatement("insert into test_fetch(value) values(?)");
    for (int i = 0; i < count; ++i) {
      stmt.setInt(1, i);
      stmt.executeUpdate();
    }
    stmt.close();
    con.commit();
  }

  // Test various fetchsizes.
  @Test
  public void testBasicFetch() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value");
    int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
    for (int i = 0; i < testSizes.length; ++i) {
      stmt.setFetchSize(testSizes[i]);
      assertEquals(testSizes[i], stmt.getFetchSize());

      ResultSet rs = stmt.executeQuery();
      assertEquals(testSizes[i], rs.getFetchSize());

      int count = 0;
      while (rs.next()) {
        assertEquals("query value error with fetch size " + testSizes[i], count, rs.getInt(1));
        ++count;
      }

      assertEquals("total query size error with fetch size " + testSizes[i], 100, count);
    }
    stmt.close();
  }

  // Similar, but for scrollable resultsets.
  @Test
  public void testScrollableFetch() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
    for (int i = 0; i < testSizes.length; ++i) {
      stmt.setFetchSize(testSizes[i]);
      assertEquals(testSizes[i], stmt.getFetchSize());

      ResultSet rs = stmt.executeQuery();
      assertEquals(testSizes[i], rs.getFetchSize());

      for (int j = 0; j <= 50; ++j) {
        assertTrue("ran out of rows at position " + j + " with fetch size " + testSizes[i], rs.next());
        assertEquals("query value error with fetch size " + testSizes[i], j, rs.getInt(1));
      }

      int position = 50;
      for (int j = 1; j < 100; ++j) {
        for (int k = 0; k < j; ++k) {
          if (j % 2 == 0) {
            ++position;
            assertTrue("ran out of rows doing a forward fetch on iteration " + j + "/" + k + " at position " + position + " with fetch size " + testSizes[i], rs.next());
          }
          else {
            --position;
            assertTrue("ran out of rows doing a reverse fetch on iteration " + j + "/" + k + " at position " + position + " with fetch size " + testSizes[i], rs.previous());
          }

          assertEquals("query value error on iteration " + j + "/" + k + " with fetch size " + testSizes[i], position, rs.getInt(1));
        }
      }

      rs.close();
    }

    stmt.close();
  }

  @Test
  public void testScrollableAbsoluteFetch() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
    for (int i = 0; i < testSizes.length; ++i) {
      stmt.setFetchSize(testSizes[i]);
      assertEquals(testSizes[i], stmt.getFetchSize());

      ResultSet rs = stmt.executeQuery();
      assertEquals(testSizes[i], rs.getFetchSize());

      int position = 50;
      assertTrue("ran out of rows doing an absolute fetch at " + position + " with fetch size " + testSizes[i], rs.absolute(position + 1));
      assertEquals("query value error with fetch size " + testSizes[i], position, rs.getInt(1));

      for (int j = 1; j < 100; ++j) {
        if (j % 2 == 0)
          position += j;
        else
          position -= j;

        assertTrue("ran out of rows doing an absolute fetch at " + position + " on iteration " + j + " with fetchsize" + testSizes[i], rs.absolute(position + 1));
        assertEquals("query value error with fetch size " + testSizes[i], position, rs.getInt(1));
      }

      rs.close();
    }

    stmt.close();
  }

  //
  // Tests for ResultSet.setFetchSize().
  //

  // test one:
  // set fetchsize = 0
  // run query (all rows should be fetched)
  // set fetchsize = 50 (should have no effect)
  // process results
  @Test
  public void testResultSetFetchSizeOne() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value");
    stmt.setFetchSize(0);
    ResultSet rs = stmt.executeQuery();
    rs.setFetchSize(50); // Should have no effect.

    int count = 0;
    while (rs.next()) {
      assertEquals(count, rs.getInt(1));
      ++count;
    }

    assertEquals(100, count);
    rs.close();
    stmt.close();
  }

  // test two:
  // set fetchsize = 25
  // run query (25 rows fetched)
  // set fetchsize = 0
  // process results:
  // process 25 rows
  // should do a FETCH ALL to get more data
  // process 75 rows
  @Test
  public void testResultSetFetchSizeTwo() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value");
    stmt.setFetchSize(25);
    ResultSet rs = stmt.executeQuery();
    rs.setFetchSize(0);

    int count = 0;
    while (rs.next()) {
      assertEquals(count, rs.getInt(1));
      ++count;
    }

    assertEquals(100, count);

    rs.close();
    stmt.close();
  }

  // test three:
  // set fetchsize = 25
  // run query (25 rows fetched)
  // set fetchsize = 50
  // process results:
  // process 25 rows. should NOT hit end-of-results here.
  // do a FETCH FORWARD 50
  // process 50 rows
  // do a FETCH FORWARD 50
  // process 25 rows. end of results.
  @Test
  public void testResultSetFetchSizeThree() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value");
    stmt.setFetchSize(25);
    ResultSet rs = stmt.executeQuery();
    rs.setFetchSize(50);

    int count = 0;
    while (rs.next()) {
      assertEquals(count, rs.getInt(1));
      ++count;
    }

    assertEquals(100, count);
    rs.close();
    stmt.close();
  }

  // test four:
  // set fetchsize = 50
  // run query (50 rows fetched)
  // set fetchsize = 25
  // process results:
  // process 50 rows.
  // do a FETCH FORWARD 25
  // process 25 rows
  // do a FETCH FORWARD 25
  // process 25 rows. end of results.
  @Test
  public void testResultSetFetchSizeFour() throws Exception {
    createRows(100);

    PreparedStatement stmt = con.prepareStatement("select * from test_fetch order by value");
    stmt.setFetchSize(50);
    ResultSet rs = stmt.executeQuery();
    rs.setFetchSize(25);

    int count = 0;
    while (rs.next()) {
      assertEquals(count, rs.getInt(1));
      ++count;
    }

    assertEquals(100, count);
    rs.close();
    stmt.close();
  }

  @Test
  public void testSingleRowResultPositioning() throws Exception {
    String msg;
    createRows(1);

    int[] sizes = {0, 1, 10};
    for (int i = 0; i < sizes.length; ++i) {
      Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      stmt.setFetchSize(sizes[i]);

      // Create a one row result set.
      ResultSet rs = stmt.executeQuery("select * from test_fetch order by value");

      msg = "before-first row positioning error with fetchsize=" + sizes[i];
      assertTrue(msg, rs.isBeforeFirst());
      assertTrue(msg, !rs.isAfterLast());
      assertTrue(msg, !rs.isFirst());
      try {
        rs.isLast();
        fail("isLast should return null for forward-only cursors");
      }
      catch (SQLFeatureNotSupportedException e) {
        // Expected...
      }

      msg = "row 1 positioning error with fetchsize=" + sizes[i];
      assertTrue(msg, rs.next());

      assertTrue(msg, !rs.isBeforeFirst());
      assertTrue(msg, !rs.isAfterLast());
      assertTrue(msg, rs.isFirst());
      try {
        rs.isLast();
        fail("isLast should return null for forward-only cursors");
      }
      catch (SQLFeatureNotSupportedException e) {
        // Expected...
      }
      assertEquals(msg, 0, rs.getInt(1));

      msg = "after-last row positioning error with fetchsize=" + sizes[i];
      assertTrue(msg, !rs.next());

      assertTrue(msg, !rs.isBeforeFirst());
      assertTrue(msg, rs.isAfterLast());
      assertTrue(msg, !rs.isFirst());
      try {
        rs.isLast();
        fail("isLast should return null for forward-only cursors");
      }
      catch (SQLFeatureNotSupportedException e) {
        // Expected...
      }

      rs.close();
      stmt.close();
    }
  }

  @Test
  public void testMultiRowResultPositioning() throws Exception {
    String msg;

    createRows(100);

    int[] sizes = {0, 1, 10, 100};
    for (int i = 0; i < sizes.length; ++i) {
      Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      stmt.setFetchSize(sizes[i]);

      ResultSet rs = stmt.executeQuery("select * from test_fetch order by value");
      msg = "before-first row positioning error with fetchsize=" + sizes[i];
      assertTrue(msg, rs.isBeforeFirst());
      assertTrue(msg, !rs.isAfterLast());
      assertTrue(msg, !rs.isFirst());
      try {
        rs.isLast();
        fail("isLast should return null for forward-only cursors");
      }
      catch (SQLFeatureNotSupportedException e) {
        // Expected...
      }

      for (int j = 0; j < 100; ++j) {
        msg = "row " + j + " positioning error with fetchsize=" + sizes[i];
        assertTrue(msg, rs.next());
        assertEquals(msg, j, rs.getInt(1));

        assertTrue(msg, !rs.isBeforeFirst());
        assertTrue(msg, !rs.isAfterLast());
        if (j == 0)
          assertTrue(msg, rs.isFirst());
        else
          assertTrue(msg, !rs.isFirst());

        try {
          rs.isLast();
          fail("isLast should return null for forward-only cursors");
        }
        catch (SQLFeatureNotSupportedException e) {
          // Expected...
        }
      }

      msg = "after-last row positioning error with fetchsize=" + sizes[i];
      assertTrue(msg, !rs.next());

      assertTrue(msg, !rs.isBeforeFirst());
      assertTrue(msg, rs.isAfterLast());
      assertTrue(msg, !rs.isFirst());
      try {
        rs.isLast();
        fail("isLast should return null for forward-only cursors");
      }
      catch (SQLFeatureNotSupportedException e) {
        // Expected...
      }

      rs.close();
      stmt.close();
    }
  }

  // Test odd queries that should not be transformed into cursor-based fetches.
  @Test
  public void testInsert() throws Exception {
    // INSERT should not be transformed.
    PreparedStatement stmt = con.prepareStatement("insert into test_fetch(value) values(1)");
    stmt.setFetchSize(100); // Should be meaningless.
    stmt.executeUpdate();
    stmt.close();
  }

  @Test
  @Ignore
  public void testMultistatement() throws Exception {
    // Queries with multiple statements should not be transformed.

    createRows(100); // 0 .. 99
    PreparedStatement stmt = con.prepareStatement("insert into test_fetch(value) values(100); select * from test_fetch order by value");
    stmt.setFetchSize(10);

    assertTrue(!stmt.execute()); // INSERT
    assertTrue(stmt.getMoreResults()); // SELECT
    ResultSet rs = stmt.getResultSet();
    int count = 0;
    while (rs.next()) {
      assertEquals(count, rs.getInt(1));
      ++count;
    }

    assertEquals(101, count);
    rs.close();
    stmt.close();
  }

  // if the driver tries to use a cursor with autocommit on
  // it will fail because the cursor will disappear partway
  // through execution
  @Test
  public void testNoCursorWithAutoCommit() throws Exception {
    createRows(10); // 0 .. 9
    con.setAutoCommit(true);
    Statement stmt = con.createStatement();
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM test_fetch ORDER BY value");
    int count = 0;
    while (rs.next()) {
      assertEquals(count++, rs.getInt(1));
    }

    assertEquals(10, count);
    rs.close();
    stmt.close();
  }

  @Test
  public void testGetRow() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("SELECT 1 UNION SELECT 2 UNION SELECT 3");
    int count = 0;
    while (rs.next()) {
      count++;
      assertEquals(count, rs.getInt(1));
      assertEquals(count, rs.getRow());
    }
    assertEquals(3, count);
    rs.close();
    stmt.close();
  }

}
