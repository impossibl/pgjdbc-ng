package com.impossibl.postgres.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
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
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class SystemIdsTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
  }

  @After
  public void after() throws SQLException {
    TestUtil.closeDB(conn);
  }

  @Test
  public void testOids() throws SQLException {

    try (Statement statement = conn.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT '2222793757'::oid")) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT '2222793757'::oid")) {
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::oid = '2222793757'::oid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setLong(1, 2222793757L);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setBigDecimal(1, new BigDecimal("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setObject(1, new BigInteger("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setString(1, "2222793757");
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
    }
    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::oid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

  }

  @Test
  public void testXids() throws SQLException {

    try (Statement statement = conn.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT '2222793757'::xid")) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT '2222793757'::xid")) {
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::xid = '2222793757'::xid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setLong(1, 2222793757L);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setBigDecimal(1, new BigDecimal("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setObject(1, new BigInteger("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setString(1, "2222793757");
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
    }
    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::xid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

  }

  @Test
  public void testCids() throws SQLException {

    try (Statement statement = conn.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT '2222793757'::cid")) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT '2222793757'::cid")) {
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::cid = '2222793757'::cid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setLong(1, 2222793757L);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setBigDecimal(1, new BigDecimal("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setObject(1, new BigInteger("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
      statement.setString(1, "2222793757");
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
      }
    }
    try (PreparedStatement statement = conn.prepareStatement("SELECT ?::cid")) {
      statement.setInt(1, Integer.parseUnsignedInt("2222793757"));
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(-2072173539, resultSet.getInt(1));
        assertEquals(2222793757L, resultSet.getLong(1));
        assertEquals(BigDecimal.valueOf(2222793757L), resultSet.getBigDecimal(1));
        assertEquals(BigInteger.valueOf(2222793757L), resultSet.getObject(1, BigInteger.class));
        assertEquals("2222793757", resultSet.getString(1));
      }
    }

  }

}
