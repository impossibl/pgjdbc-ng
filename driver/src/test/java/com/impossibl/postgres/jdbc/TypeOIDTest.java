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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public class TypeOIDTest {

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
  public void testUnsignedOid() throws SQLException {
    assumeTrue("Server version is 12 or above", conn.getMetaData().getDatabaseMajorVersion() >= 12);

    try (Connection conn = TestUtil.openDB()) {
      try (Statement statement = conn.createStatement()) {
        statement.execute("ALTER SYSTEM SET allow_system_table_mods = 'on'");
        statement.execute("SELECT pg_reload_conf()");
      }

      TestUtil.createType(conn, "tester", "value text");
      try {

        try (Statement statement = conn.createStatement()) {
          int typeOid;
          try (ResultSet rs = statement.executeQuery("SELECT oid FROM pg_type WHERE typname = 'tester'")) {
            assertTrue(rs.next());
            typeOid = rs.getInt(1);
          }

          String unsignedTypeOid = Integer.toUnsignedString((int) ((long) Integer.MIN_VALUE + 1024));
          statement.executeUpdate("UPDATE pg_type SET oid = " + unsignedTypeOid + " WHERE oid = " + typeOid);
          statement.executeUpdate("DELETE FROM pg_type WHERE typelem = " + typeOid); // clean up orphaned array type

          assertNotNull(conn.unwrap(PGConnection.class).resolveType("tester"));
        }

      }
      finally {
        TestUtil.dropType(conn, "tester");
      }
    }
  }

}
