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

    if (conn != null)
      getHousekeeper().testClear();
  }

  Housekeeper getHousekeeper() {
    return ((PGConnection) conn).housekeeper;
  }

  @Test
  public void testResultSetLeak() throws SQLException {

    Housekeeper housekeeper = getHousekeeper();

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

    Housekeeper housekeeper = getHousekeeper();

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

    Housekeeper housekeeper = getHousekeeper();

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

    Housekeeper housekeeper = getHousekeeper();

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

    Housekeeper housekeeper = getHousekeeper();

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

    Housekeeper housekeeper = getHousekeeper();

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
          assertTrue(housekeeper instanceof NullHousekeeper);

        }
      }
    }

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
