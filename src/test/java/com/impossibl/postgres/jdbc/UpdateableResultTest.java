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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



@RunWith(JUnit4.class)
public class UpdateableResultTest {

  private Connection con;

  @Before
  public void setUp() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "updateable", "id int primary key, name text, notselected text, ts timestamp with time zone, intarr int[]", true);
    TestUtil.createTable(con, "second", "id1 int primary key, name1 text");
    TestUtil.createTable(con, "stream", "id int primary key, asi text, chr text, bin bytea");

    // put some dummy data into second
    Statement st2 = con.createStatement();
    st2.execute("insert into second values (1,'anyvalue' )");
    st2.close();

    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    con.setAutoCommit(false);
  }

  @After
  public void tearDown() throws Exception {
    con.commit();

    TestUtil.dropTable(con, "updateable");
    TestUtil.dropTable(con, "second");
    TestUtil.dropTable(con, "stream");
    TestUtil.closeDB(con);
  }

  @Test
  @Ignore
  public void testDeleteRows() throws SQLException {
    Statement st = con.createStatement();
    st.executeUpdate("INSERT INTO second values (2,'two')");
    st.executeUpdate("INSERT INTO second values (3,'three')");
    st.executeUpdate("INSERT INTO second values (4,'four')");
    st.close();

    st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select id1,name1 from second order by id1");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt("id1"));
    rs.deleteRow();
    assertTrue(rs.isBeforeFirst());

    assertTrue(rs.next());
    assertTrue(rs.next());
    assertEquals(3, rs.getInt("id1"));
    rs.deleteRow();
    assertEquals(2, rs.getInt("id1"));

    rs.close();
    st.close();
  }

  @Test
  @Ignore
  public void testCancelRowUpdates() throws Exception {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select * from second");

    // make sure we're dealing with the correct row.
    rs.first();
    assertEquals(1, rs.getInt(1));
    assertEquals("anyvalue", rs.getString(2));

    // update, cancel and make sure nothings changed.
    rs.updateInt(1, 99);
    rs.cancelRowUpdates();
    assertEquals(1, rs.getInt(1));
    assertEquals("anyvalue", rs.getString(2));

    // real update
    rs.updateInt(1, 999);
    rs.updateRow();
    assertEquals(999, rs.getInt(1));
    assertEquals("anyvalue", rs.getString(2));

    // scroll some and make sure the update is still there
    rs.beforeFirst();
    rs.next();
    assertEquals(999, rs.getInt(1));
    assertEquals("anyvalue", rs.getString(2));

    // make sure the update got to the db and the driver isn't lying to us.
    rs.close();
    rs = st.executeQuery("select * from second");
    rs.first();
    assertEquals(999, rs.getInt(1));
    assertEquals("anyvalue", rs.getString(2));

    rs.close();
    st.close();
  }

  private void checkPositioning(ResultSet rs) throws SQLException {
    try {
      rs.getInt(1);
      fail("Can't use an incorrectly positioned result set.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    try {
      rs.updateInt(1, 2);
      fail("Can't use an incorrectly positioned result set.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    try {
      rs.updateRow();
      fail("Can't use an incorrectly positioned result set.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    try {
      rs.deleteRow();
      fail("Can't use an incorrectly positioned result set.");
    }
    catch (SQLException sqle) {
      // Expected...
    }
  }

  @Test
  public void testPositioning() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = stmt.executeQuery("SELECT id1,name1 FROM second");

    checkPositioning(rs);

    assertTrue(rs.next());
    rs.beforeFirst();
    checkPositioning(rs);

    rs.afterLast();
    checkPositioning(rs);

    rs.beforeFirst();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    checkPositioning(rs);

    rs.afterLast();
    assertTrue(rs.previous());
    assertTrue(!rs.previous());
    checkPositioning(rs);

    rs.close();
    stmt.close();
  }

  @Test
  @Ignore
  public void testUpdateTimestamp() throws SQLException {
    TimeZone origTZ = TimeZone.getDefault();
    try {
      // We choose a timezone which has a partial hour portion
      // Asia/Tehran is +3:30
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tehran"));
      Timestamp ts = Timestamp.valueOf("2006-11-20 16:17:18");

      Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
      ResultSet rs = stmt.executeQuery("SELECT id, ts FROM updateable");
      rs.moveToInsertRow();
      rs.updateInt(1, 1);
      rs.updateTimestamp(2, ts);
      rs.insertRow();
      assertTrue(rs.first());
      assertEquals(ts, rs.getTimestamp(2));
    }
    finally {
      TimeZone.setDefault(origTZ);
    }
  }

  @Test
  @Ignore
  public void testUpdateStreams() throws SQLException, UnsupportedEncodingException {
    String string = "Hello";
    byte[] bytes = new byte[] {0, '\\', (byte) 128, (byte) 255};

    Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = stmt.executeQuery("SELECT id, asi, chr, bin FROM stream");

    rs.moveToInsertRow();
    rs.updateInt(1, 1);
    rs.updateAsciiStream("asi", null, 17);
    rs.updateCharacterStream("chr", null, 81);
    rs.updateBinaryStream("bin", null, 0);
    rs.insertRow();

    rs.moveToInsertRow();
    rs.updateInt(1, 3);
    rs.updateAsciiStream("asi", new ByteArrayInputStream(string.getBytes("US-ASCII")), 5);
    rs.updateCharacterStream("chr", new StringReader(string), 5);
    rs.updateBinaryStream("bin", new ByteArrayInputStream(bytes), bytes.length);
    rs.insertRow();

    rs.beforeFirst();
    assertTrue(rs.next());

    assertEquals(1, rs.getInt(1));
    assertNull(rs.getString(2));
    assertNull(rs.getString(3));
    assertNull(rs.getBytes(4));

    rs.updateInt("id", 2);
    rs.updateAsciiStream("asi", new ByteArrayInputStream(string.getBytes("US-ASCII")), 5);
    rs.updateCharacterStream("chr", new StringReader(string), 5);
    rs.updateBinaryStream("bin", new ByteArrayInputStream(bytes), bytes.length);
    rs.updateRow();

    assertEquals(2, rs.getInt(1));
    assertEquals(string, rs.getString(2));
    assertEquals(string, rs.getString(3));
    assertTrue(Arrays.equals(bytes, rs.getBytes(4)));

    rs.refreshRow();

    assertEquals(2, rs.getInt(1));
    assertEquals(string, rs.getString(2));
    assertEquals(string, rs.getString(3));
    assertTrue(Arrays.equals(bytes, rs.getBytes(4)));

    assertTrue(rs.next());

    assertEquals(3, rs.getInt(1));
    assertEquals(string, rs.getString(2));
    assertEquals(string, rs.getString(3));
    assertTrue(Arrays.equals(bytes, rs.getBytes(4)));

    rs.close();
    stmt.close();
  }

  @Test
  public void testZeroRowResult() throws SQLException {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select * from updateable WHERE 0 > 1");
    assertTrue(!rs.next());
    rs.moveToInsertRow();
    rs.moveToCurrentRow();
  }

  @Test
  @Ignore
  public void testUpdateable() throws SQLException {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    ResultSet rs = st.executeQuery("select * from updateable");
    assertNotNull(rs);
    rs.moveToInsertRow();
    rs.updateInt(1, 1);
    rs.updateString(2, "jake");
    rs.updateString(3, "avalue");
    rs.insertRow();
    assertTrue(rs.first());

    rs.updateInt("id", 2);
    rs.updateString("name", "dave");
    rs.updateRow();

    assertEquals(2, rs.getInt("id"));
    assertEquals("dave", rs.getString("name"));
    assertEquals("avalue", rs.getString("notselected"));

    rs.deleteRow();
    rs.moveToInsertRow();
    rs.updateInt("id", 3);
    rs.updateString("name", "paul");

    rs.insertRow();

    try {
      rs.refreshRow();
      fail("Can't refresh when on the insert row.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    assertEquals(3, rs.getInt("id"));
    assertEquals("paul", rs.getString("name"));
    assertNull(rs.getString("notselected"));

    rs.close();

    rs = st.executeQuery("select id1, id, name, name1 from updateable, second");
    try {
      while (rs.next()) {
        rs.updateInt("id", 2);
        rs.updateString("name", "dave");
        rs.updateRow();
      }

      fail("should not get here, update should fail");
    }
    catch (SQLException ex) {
      // Expected...
    }

    rs = st.executeQuery("select oid,* from updateable");
    assertTrue(rs.first());
    rs.updateInt("id", 3);
    rs.updateString("name", "dave3");
    rs.updateRow();
    assertEquals(3, rs.getInt("id"));
    assertEquals("dave3", rs.getString("name"));

    rs.moveToInsertRow();
    rs.updateInt("id", 4);
    rs.updateString("name", "dave4");

    rs.insertRow();
    rs.updateInt("id", 5);
    rs.updateString("name", "dave5");
    rs.insertRow();

    rs.moveToCurrentRow();
    assertEquals(3, rs.getInt("id"));
    assertEquals("dave3", rs.getString("name"));

    assertTrue(rs.next());
    assertEquals(4, rs.getInt("id"));
    assertEquals("dave4", rs.getString("name"));

    assertTrue(rs.next());
    assertEquals(5, rs.getInt("id"));
    assertEquals("dave5", rs.getString("name"));

    rs.close();
    st.close();
  }

  @Test
  public void testInsertRowIllegalMethods() throws Exception {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select * from updateable");
    assertNotNull(rs);
    rs.moveToInsertRow();

    try {
      rs.cancelRowUpdates();
      fail("expected an exception when calling cancelRowUpdates() on the insert row");
    }
    catch (SQLException e) {
      // Expected...
    }

    try {
      rs.updateRow();
      fail("expected an exception when calling updateRow() on the insert row");
    }
    catch (SQLException e) {
      // Expected...
    }

    try {
      rs.deleteRow();
      fail("expected an exception when calling deleteRow() on the insert row");
    }
    catch (SQLException e) {
      // Expected...
    }

    try {
      rs.refreshRow();
      fail("expected an exception when calling refreshRow() on the insert row");
    }
    catch (SQLException e) {
      // Expected...
    }

    rs.close();
    st.close();
  }

  @Test
  public void testUpdateablePreparedStatement() throws Exception {
    // No args.
    PreparedStatement st = con.prepareStatement("select * from updateable", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery();
    rs.moveToInsertRow();
    rs.close();
    st.close();

    // With args.
    st = con.prepareStatement("select * from updateable where id = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    st.setInt(1, 1);
    rs = st.executeQuery();
    rs.moveToInsertRow();
    rs.close();
    st.close();
  }

  @Test
  public void testUpdateSelectOnly() throws Exception {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

    ResultSet rs = st.executeQuery("select * from only second");
    assertTrue(rs.next());
    rs.updateInt(1, 2);
    rs.updateRow();
  }

  @Test
  public void testUpdateReadOnlyResultSet() throws Exception {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = st.executeQuery("select * from updateable");
    try {
      rs.moveToInsertRow();
      fail("expected an exception when calling moveToInsertRow() on a read-only resultset");
    }
    catch (SQLException e) {
      // Expected...
    }
  }

  @Test
  public void testBadColumnIndexes() throws Exception {
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select * from updateable");
    rs.moveToInsertRow();
    try {
      rs.updateInt(0, 1);
      fail("Should have thrown an exception on bad column index.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    try {
      rs.updateString(1000, "hi");
      fail("Should have thrown an exception on bad column index.");
    }
    catch (SQLException sqle) {
      // Expected...
    }

    try {
      rs.updateNull(1000);
      fail("Should have thrown an exception on bad column index.");
    }
    catch (SQLException sqle) {
      // Expected...
    }
  }

  @Test
  public void testArray() throws SQLException {
    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    stmt.executeUpdate("INSERT INTO updateable (id, intarr) VALUES (1, '{1,2,3}'::int4[])");
    ResultSet rs = stmt.executeQuery("SELECT id, intarr FROM updateable");
    assertTrue(rs.next());
    rs.updateObject(2, rs.getArray(2));
    rs.updateRow();

    Array arr = rs.getArray(2);
    assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());
    rs.close();

    rs = stmt.executeQuery("SELECT id,intarr FROM updateable");
    assertTrue(rs.next());
    arr = rs.getArray(2);
    assertEquals(Types.INTEGER, arr.getBaseType());
    intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());

    rs.close();
    stmt.close();
  }

}
