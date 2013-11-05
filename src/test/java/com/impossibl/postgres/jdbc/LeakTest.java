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

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.Boolean.FALSE;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;



@RunWith(JUnit4.class)
public class LeakTest {

  WeakReference<Connection> connRef;
  Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    connRef = new WeakReference<Connection>(conn);
  }

  @After
  public void after() throws Exception {
    if (conn != null && getHousekeeper() != null)
      getHousekeeper().testClear();
  }

  ThreadedHousekeeper getHousekeeper() {
    if (((PGConnection) conn).housekeeper != null)
      return (ThreadedHousekeeper)((PGConnection) conn).housekeeper;
    else
      return null;
  }

  @Test
  public void testResultSetLeak() throws SQLException {

    ThreadedHousekeeper housekeeper = getHousekeeper();

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(connId));
  }


  @Test
  public void testResultSetNoLeak() throws SQLException {

    ThreadedHousekeeper housekeeper = getHousekeeper();

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs.close();

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testStatementLeak() throws SQLException {

    ThreadedHousekeeper housekeeper = getHousekeeper();

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;
    stmt = null;

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testStatementNoLeak() throws SQLException {

    ThreadedHousekeeper housekeeper = getHousekeeper();

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs.close();
    stmt.close();

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testConnectionLeak() throws SQLException {

    ThreadedHousekeeper housekeeper = getHousekeeper();

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;
    stmt = null;
    conn = null;

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testConnectionNoLeak() throws SQLException {

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs.close();
    stmt.close();
    conn.close();

    ThreadedHousekeeper housekeeper = getHousekeeper();

    sleep();
    assertTrue(housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertTrue(housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testNoHousekeeper() throws Exception {

    Properties settings = new Properties();
    settings.setProperty("housekeeper.enabled", FALSE.toString());

    try (Connection conn = TestUtil.openDB(settings)) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT 1")) {

          Housekeeper housekeeper = ((PGConnection) conn).housekeeper;
          assertNull(housekeeper);

        }
      }
    }

  }

  private void sleep() {
    System.gc();
    try {
      Thread.sleep(50);
    }
    catch (InterruptedException e) {
      // Ignore...
    }
  }

}
