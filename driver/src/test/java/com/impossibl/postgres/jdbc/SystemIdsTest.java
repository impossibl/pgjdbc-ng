/**
 * Copyright (c) 2020, impossibl.com
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
