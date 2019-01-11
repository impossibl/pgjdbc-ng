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
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class GeneratedKeysTest {

  private Connection _conn;

  @Before
  public void before() throws Exception {
    _conn = TestUtil.openDB();
    Statement stmt = _conn.createStatement();
    stmt.execute("CREATE TEMP TABLE genkeys(a serial, b text, c int)");
    stmt.execute("CREATE TEMP TABLE genkeys2(\"aA\" serial, \"bbbBbb\" text, \"c\" int)");
    stmt.close();
  }

  @After
  public void after() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.execute("DROP TABLE genkeys");
    stmt.execute("DROP TABLE genkeys2");
    stmt.close();
    TestUtil.closeDB(_conn);
  }

  @Test
  public void testExecuteUpdateAllColumns() throws SQLException {
    Statement stmt = _conn.createStatement();
    int count = stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)", Statement.RETURN_GENERATED_KEYS);
    assertEquals(1, count);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("a", rs.getString(2));
    assertEquals(2, rs.getInt(3));
    assertEquals(1, rs.getInt("a"));
    assertEquals("a", rs.getString("b"));
    assertEquals(2, rs.getInt("c"));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testStatementUpdateCount() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)", Statement.RETURN_GENERATED_KEYS);
    assertEquals(1, stmt.getUpdateCount());
    assertNull(stmt.getResultSet());
    assertTrue(!stmt.getMoreResults());
    stmt.close();
  }

  @Test
  public void testCloseStatementClosesRS() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)", Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = stmt.getGeneratedKeys();
    stmt.close();
    try {
      rs.next();
      fail("Can't operate on a closed result set.");
    }
    catch (SQLException sqle) {
      // Ok
    }
    finally {
      rs.close();
    }
  }

  @Test
  public void testReturningWithTrailingSemicolon() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2); ", Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals("a", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testEmptyRSWithoutReturning() throws SQLException {
    Statement stmt = _conn.createStatement();
    int count = stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2); ", Statement.NO_GENERATED_KEYS);
    assertEquals(1, count);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testColumnsByName() throws SQLException {
    Statement stmt = _conn.createStatement();
    int count = stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2); ", new String[] {"c", "a"});
    assertEquals(1, count);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(1, rs.getInt(2));
    assertEquals(1, rs.getInt("a"));
    assertEquals(2, rs.getInt("c"));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testMultipleRows() throws SQLException {
    Statement stmt = _conn.createStatement();
    int count = stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2), (3, 'b', 4); ", new String[] {"c"});
    assertEquals(2, count);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testSerialWorks() throws SQLException {
    Statement stmt = _conn.createStatement();
    int count = stmt.executeUpdate("INSERT INTO genkeys (b,c) VALUES ('a', 2), ('b', 4); ", new String[] {"a"});
    assertEquals(2, count);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testUpdate() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)");
    stmt.executeUpdate("INSERT INTO genkeys VALUES (2, 'b', 4)");
    stmt.executeUpdate("UPDATE genkeys SET c=3 WHERE a = 1", new String[] {"c", "b"});
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals("a", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testDelete() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)");
    stmt.executeUpdate("INSERT INTO genkeys VALUES (2, 'b', 4)");
    stmt.executeUpdate("DELETE FROM genkeys WHERE a = 1", new String[] {"c", "b"});
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("a", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testPSUpdate() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)");
    stmt.executeUpdate("INSERT INTO genkeys VALUES (2, 'b', 4)");
    stmt.close();

    PreparedStatement ps = _conn.prepareStatement("UPDATE genkeys SET c=? WHERE a = ?", new String[] {"c", "b"});
    ps.setInt(1, 3);
    ps.setInt(2, 1);
    assertEquals(1, ps.executeUpdate());
    ResultSet rs = ps.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals("a", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();
    ps.close();
  }

  @Test
  public void testPSDelete() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2)");
    stmt.executeUpdate("INSERT INTO genkeys VALUES (2, 'b', 4)");
    stmt.close();

    PreparedStatement ps = _conn.prepareStatement("DELETE FROM genkeys WHERE a = ?", new String[] {"c", "b"});

    ps.setInt(1, 1);
    assertEquals(1, ps.executeUpdate());
    ResultSet rs = ps.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("a", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();

    ps.setInt(1, 2);
    assertEquals(1, ps.executeUpdate());
    rs = ps.getGeneratedKeys();
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals("b", rs.getString(2));
    assertTrue(!rs.next());
    rs.close();

    ps.close();
  }

  @Test
  public void testGeneratedKeysCleared() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (1, 'a', 2); ", Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = stmt.getGeneratedKeys();
    assertTrue(rs.next());
    rs.close();
    stmt.executeUpdate("INSERT INTO genkeys VALUES (2, 'b', 3)");
    rs = stmt.getGeneratedKeys();
    assertTrue(!rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testBatchGeneratedKeys() throws SQLException {
    PreparedStatement ps = _conn.prepareStatement("INSERT INTO genkeys(c) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
    ps.setInt(1, 4);
    ps.addBatch();
    ps.setInt(1, 7);
    ps.addBatch();
    ps.executeBatch();
    ResultSet batchRs1 = ps.getGeneratedKeys();
    assertTrue(batchRs1.next());
    assertEquals(1, batchRs1.getInt("a"));
    assertTrue(batchRs1.next());
    assertEquals(2, batchRs1.getInt("a"));
    assertTrue(!batchRs1.next());
    // BatchRs1 isn't explicit closed
    assertFalse(batchRs1.isClosed());

    ps.setInt(1, 4);
    ps.addBatch();
    ps.setInt(1, 7);
    ps.addBatch();
    ps.executeBatch();
    ResultSet batchRs2 = ps.getGeneratedKeys();
    assertTrue(batchRs2.next());
    assertEquals(3, batchRs2.getInt("a"));
    assertTrue(batchRs2.next());
    assertEquals(4, batchRs2.getInt("a"));
    assertTrue(!batchRs2.next());
    batchRs2.close();

    assertTrue(batchRs1.isClosed());
    assertTrue(batchRs2.isClosed());

    ps.close();
  }

  @Test
  public void testGeneratedKeysCapitalizedColumnNames() throws SQLException {
    try (Statement stmt = _conn.createStatement()) {
      stmt.executeUpdate("INSERT INTO genkeys2 VALUES (1, 'a', 2); ", new String[] {"c", "bbbBbb", "aA"});
      try (ResultSet rs = stmt.getGeneratedKeys()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("aA"));
        assertEquals("a", rs.getString("bbbBbb"));
        assertEquals(2, rs.getInt("c"));
      }

      stmt.executeUpdate("INSERT INTO genkeys2 VALUES (1, 'a', 2); ", Statement.RETURN_GENERATED_KEYS);
      try (ResultSet rs = stmt.getGeneratedKeys()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("aA"));
      }
    }
  }

}
