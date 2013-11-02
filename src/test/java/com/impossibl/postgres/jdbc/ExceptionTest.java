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
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;



public class ExceptionTest {

  Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "checktest", "id integer PRIMARY KEY, amount numeric CHECK (amount > 0), val integer NOT NULL, num integer UNIQUE");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(conn, "checktest");
  }

  @Test(expected = SQLIntegrityConstraintViolationException.class)
  public void testCheckConstraintExceptionType() throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      stmt.executeUpdate("INSERT INTO checktest(amount) VALUES (-100)");

    }

  }

  @Test(expected = SQLIntegrityConstraintViolationException.class)
  public void testUniqueConstraintExceptionType() throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      stmt.executeUpdate("INSERT INTO checktest(num) VALUES (5)");
      stmt.executeUpdate("INSERT INTO checktest(num) VALUES (5)");

    }

  }

  @Test(expected = SQLIntegrityConstraintViolationException.class)
  public void testNotNullConstraintExceptionType() throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      stmt.executeUpdate("INSERT INTO checktest(val) VALUES (null)");

    }

  }

  @Test(expected = SQLIntegrityConstraintViolationException.class)
  public void testPrimaryKeyConstraintExceptionType() throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      stmt.executeUpdate("INSERT INTO checktest(id) VALUES (null)");

    }

  }

}
