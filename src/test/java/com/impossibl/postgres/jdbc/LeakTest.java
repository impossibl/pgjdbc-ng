package com.impossibl.postgres.jdbc;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertFalse;
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
    Housekeeper.testClear();
  }

  @Test
  public void testResultSetLeak() throws SQLException {

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;

    sleep();
    assertTrue(Housekeeper.testCheckCleaned(rsId));
    sleep();
    assertFalse(Housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(Housekeeper.testCheckCleaned(connId));
  }


  @Test
  public void testStatementLeak() throws SQLException {

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;
    stmt = null;

    sleep();
    assertTrue(Housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(Housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertFalse(Housekeeper.testCheckCleaned(connId));
  }

  @Test
  public void testConnectionLeak() throws SQLException {

    int connId = System.identityHashCode(conn);

    Statement stmt = conn.createStatement();
    int stmtId = System.identityHashCode(stmt);

    ResultSet rs = stmt.executeQuery("SELECT 1");
    int rsId = System.identityHashCode(rs);

    rs = null;
    stmt = null;
    conn = null;

    sleep();
    assertTrue(Housekeeper.testCheckCleaned(rsId));
    sleep();
    assertTrue(Housekeeper.testCheckCleaned(stmtId));
    sleep();
    assertTrue(Housekeeper.testCheckCleaned(connId));
  }

  private void sleep() {
    System.gc();
    try {
      Thread.sleep(10);
    }
    catch (InterruptedException e) {
      // Ignore...
    }
  }

}
