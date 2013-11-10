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

import com.impossibl.postgres.api.jdbc.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class WrapperTest {

  private Connection _conn;
  private Statement _statement;

  @Before
  public void before() throws Exception {
    _conn = TestUtil.openDB();
    _statement = _conn.prepareStatement("SELECT 1");
  }

  @After
  public void after() throws SQLException {
    _statement.close();
    TestUtil.closeDB(_conn);
  }

  /**
   * This interface is private, and so cannot be supported by any wrapper
   *
   */
  private interface PrivateInterface {
  };

  @Test
  public void testConnectionIsWrapperForPrivate() throws SQLException {
    assertFalse(_conn.isWrapperFor(PrivateInterface.class));
  }

  @Test
  public void testConnectionIsWrapperForConnection() throws SQLException {
    assertTrue(_conn.isWrapperFor(Connection.class));
  }

  @Test
  public void testConnectionIsWrapperForPGConnection() throws SQLException {
    assertTrue(_conn.isWrapperFor(PGConnection.class));
  }

  @Test
  public void testConnectionUnwrapPrivate() throws SQLException {
    try {
      _conn.unwrap(PrivateInterface.class);
      fail("unwrap of non-wrapped interface should fail");
    }
    catch (SQLException e) {
      // Ok
    }
  }

  @Test
  public void testConnectionUnwrapConnection() throws SQLException {
    Object v = _conn.unwrap(Connection.class);
    assertNotNull(v);
    assertTrue(v instanceof Connection);
  }

  @Test
  public void testConnectionUnwrapPGConnection() throws SQLException {
    Object v = _conn.unwrap(PGConnection.class);
    assertNotNull(v);
    assertTrue(v instanceof PGConnection);
  }

  @Test
  public void testStatementIsWrapperForPrivate() throws SQLException {
    assertFalse(_statement.isWrapperFor(PrivateInterface.class));
  }

  @Test
  public void testStatementIsWrapperForStatement() throws SQLException {
    assertTrue(_statement.isWrapperFor(Statement.class));
  }

  @Test
  public void testStatementIsWrapperForPGStatement() throws SQLException {
    assertTrue(_statement.isWrapperFor(PGStatement.class));
  }

  @Test
  public void testStatementUnwrapPrivate() throws SQLException {
    try {
      _statement.unwrap(PrivateInterface.class);
      fail("unwrap of non-wrapped interface should fail");
    }
    catch (SQLException e) {
      // Ok
    }
  }

  @Test
  public void testStatementUnwrapStatement() throws SQLException {
    Object v = _statement.unwrap(Statement.class);
    assertNotNull(v);
    assertTrue(v instanceof Statement);
  }

  @Test
  public void testStatementUnwrapPGStatement() throws SQLException {
    Object v = _statement.unwrap(PGStatement.class);
    assertNotNull(v);
    assertTrue(v instanceof PGStatement);
  }

}
