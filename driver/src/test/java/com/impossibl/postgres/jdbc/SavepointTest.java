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
import java.sql.Savepoint;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class SavepointTest {

  private Connection _conn;

  @Before
  public void before() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "savepointtable", "id int primary key");
    TestUtil.createTable(_conn, "savepointtable2", "id int primary key");
    _conn.setAutoCommit(false);
  }

  @After
  public void after() throws SQLException {
    _conn.setAutoCommit(true);
    TestUtil.dropTable(_conn, "savepointtable");
    TestUtil.dropTable(_conn, "savepointtable2");
    TestUtil.closeDB(_conn);
  }

  private void addRow(int id) throws SQLException {
    PreparedStatement pstmt = null;
    try {
      pstmt = _conn.prepareStatement("INSERT INTO savepointtable VALUES (?)");
      pstmt.setInt(1, id);
      pstmt.executeUpdate();
    }
    finally {
      if (pstmt != null)
        pstmt.close();
    }
  }

  private int countRows() throws SQLException {
    Statement stmt = _conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM savepointtable");
    rs.next();
    int count = rs.getInt(1);
    rs.close();
    stmt.close();
    return count;
  }

  @Test
  public void testAutoCommitFails() throws SQLException {

    _conn.setAutoCommit(true);

    try {
      _conn.setSavepoint();
      fail("Can't create a savepoint with autocommit.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    try {
      _conn.setSavepoint("spname");
      fail("Can't create a savepoint with autocommit.");
    }
    catch (SQLException sqle) {
      // Ok
    }
  }

  @Test
  public void testCantMixSavepointTypes() throws SQLException {

    Savepoint namedSavepoint = _conn.setSavepoint("named");
    Savepoint unNamedSavepoint = _conn.setSavepoint();

    try {
      namedSavepoint.getSavepointId();
      fail("Can't get id from named savepoint.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    try {
      unNamedSavepoint.getSavepointName();
      fail("Can't get name from unnamed savepoint.");
    }
    catch (SQLException sqle) {
      // Ok
    }

  }

  @Test
  public void testRollingBackToSavepoints() throws SQLException {

    Savepoint empty = _conn.setSavepoint();
    addRow(1);
    Savepoint onerow = _conn.setSavepoint("onerow");
    addRow(2);

    assertEquals(2, countRows());
    _conn.rollback(onerow);
    assertEquals(1, countRows());
    _conn.rollback(empty);
    assertEquals(0, countRows());
  }

  @Test
  public void testGlobalRollbackWorks() throws SQLException {

    _conn.setSavepoint();
    addRow(1);
    _conn.setSavepoint("onerow");
    addRow(2);

    assertEquals(2, countRows());
    _conn.rollback();
    assertEquals(0, countRows());
  }

  @Test
  public void testContinueAfterError() throws SQLException {

    addRow(1);
    Savepoint savepoint = _conn.setSavepoint();
    try {
      addRow(1);
      fail("Should have thrown duplicate key exception");
    }
    catch (SQLException sqle) {
      _conn.rollback(savepoint);
    }

    assertEquals(1, countRows());
    addRow(2);
    assertEquals(2, countRows());
  }

  @Test
  public void testReleaseSavepoint() throws SQLException {

    Savepoint savepoint = _conn.setSavepoint("mysavepoint");
    _conn.releaseSavepoint(savepoint);
    try {
      savepoint.getSavepointName();
      fail("Can't use savepoint after release.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    savepoint = _conn.setSavepoint();
    _conn.releaseSavepoint(savepoint);
    try {
      savepoint.getSavepointId();
      fail("Can't use savepoint after release.");
    }
    catch (SQLException sqle) {
      // Ok
    }
  }

  @Test
  public void testComplicatedSavepointName() throws SQLException {

    Savepoint savepoint = _conn.setSavepoint("name with spaces + \"quotes\"");
    _conn.rollback(savepoint);
    _conn.releaseSavepoint(savepoint);
  }

  @Test
  public void testRollingBackToInvalidSavepointFails() throws SQLException {

    Savepoint sp1 = _conn.setSavepoint();
    Savepoint sp2 = _conn.setSavepoint();

    _conn.rollback(sp1);
    try {
      _conn.rollback(sp2);
      fail("Can't rollback to a savepoint that's invalid.");
    }
    catch (SQLException sqle) {
      // Ok
    }
  }

  @Test
  public void testRollbackMultipleTimes() throws SQLException {

    addRow(1);
    Savepoint savepoint = _conn.setSavepoint();

    addRow(2);
    _conn.rollback(savepoint);
    assertEquals(1, countRows());

    _conn.rollback(savepoint);
    assertEquals(1, countRows());

    addRow(2);
    _conn.rollback(savepoint);
    assertEquals(1, countRows());

    _conn.releaseSavepoint(savepoint);
    assertEquals(1, countRows());
  }

  @Test
  public void testRollbackToSavePointWithFetchCount() throws SQLException {
    _conn.setAutoCommit(false);

    try (Statement stmt = _conn.createStatement()) {
      stmt.execute("insert into savepointtable values (1), (2), (3);");
      stmt.execute("insert into savepointtable2 values (1), (2), (3);");
    }

    Savepoint savepoint = _conn.setSavepoint();

    try (
        PreparedStatement preparedStatement =
            _conn.prepareStatement("SELECT * from savepointtable where id = (select id from savepointtable2);")
    ) {
      preparedStatement.setFetchSize(1000);

      try (ResultSet ignored = preparedStatement.executeQuery()) {
        fail("Query should have failed");
      }

      fail("Query should have failed");
    }
    catch (Exception e) {
      // Error is expected...
      _conn.rollback(savepoint);
    }
    finally {
      try {
        _conn.setAutoCommit(true);
      }
      catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  @Test
  public void testRollbackWithFetchCount() throws Exception {
    _conn.setAutoCommit(false);

    try (Statement stmt = _conn.createStatement()) {
      stmt.execute("insert into savepointtable values (1), (2), (3);");
      stmt.execute("insert into savepointtable2 values (1), (2), (3);");
    }

    try (PreparedStatement preparedStatement =
             _conn.prepareStatement("SELECT * from savepointtable where id = (select id from savepointtable2);")
    ) {
      preparedStatement.setFetchSize(1000);

      try (ResultSet set = preparedStatement.executeQuery()) {
        while (set.next()) {
          set.getInt(1);
        }
      }

    }
    catch (Exception e) {
      // Error is expected...
      _conn.rollback();
    }
    finally {
      try {
        _conn.setAutoCommit(true);
      }
      catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

}
