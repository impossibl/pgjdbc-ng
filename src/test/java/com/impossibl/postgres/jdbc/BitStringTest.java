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
import java.sql.Types;
import java.util.BitSet;
import java.util.UUID;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BitStringTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
  }

  @After
  public void after() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS bitstringtest");
    stmt.execute("DROP TABLE IF EXISTS hexbitstringtest");
    stmt.close();
    TestUtil.closeDB(conn);
  }

  @Test
  public void testBitString() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TEMP TABLE bitstringtest(bitstring bit(5))");
    stmt.close();


    String bitString = "01001";
    PreparedStatement ps = conn.prepareStatement("INSERT INTO bitstringtest VALUES (?)");
    ps.setObject(1, "B" + bitString, Types.OTHER);
    ps.executeUpdate();
    ps.close();

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bitstring FROM bitstringtest");
    assertTrue(rs.next());
    assertEquals(bitString, rs.getString(1));

    rs.close();
    stmt.close();
  }

  @Test
  public void testHexEncodedBitString() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TEMP TABLE hexbitstringtest(bitstring bit(8))");
    stmt.close();


    String bitString = "X3B";
    PreparedStatement ps = conn.prepareStatement("INSERT INTO hexbitstringtest VALUES (?)");
    ps.setObject(1, bitString, Types.OTHER);
    ps.executeUpdate();
    ps.close();

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT bitstring FROM hexbitstringtest");
    assertTrue(rs.next());
    assertEquals("00111011", rs.getString(1));

    rs.close();
    stmt.close();
  }
}
