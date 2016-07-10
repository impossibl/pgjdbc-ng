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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BoundParametersAndFieldsLimitTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "person", "id bigint primary key");
  }

  @After
  public void after() throws SQLException {
    TestUtil.dropTable(conn, "person");
    TestUtil.closeDB(conn);
  }

  @Test(expected = PGSQLSimpleException.class)
  public void testTooManyBindParams() throws SQLException {
    testParams(0x10000);
  }

  @Test
  public void testMaxBindParams() throws SQLException {
    testParams(0xffff);
  }

  void testParams(int params) throws SQLException {

    char[] fisk = new char[params];
    for (int i = 0; i < fisk.length; i++) {
      fisk[i] = '?';
    }
    String csv = Arrays.toString(fisk).substring(1);
    csv = csv.substring(0, csv.length() - 1);
    String sql = String.format("SELECT 1 FROM person p WHERE p.id IN (%s)", csv);
    PreparedStatement ps = conn.prepareStatement(sql);
    for (int i = 1; i < fisk.length + 1; i++) {
      ps.setLong(i, i);
    }
    ps.executeQuery();
    ps.close();
  }

  @Test(expected = PGSQLSimpleException.class)
  public void testTooManyFields() throws SQLException {
    testFields(0x10000);
  }

  @Test
  public void testMaxFields() throws SQLException {
    testFields(1664);
  }

  void testFields(int fields) throws SQLException {

    char[] fisk = new char[fields];
    for (int i = 0; i < fisk.length; i++) {
      fisk[i] = '1';
    }
    String csv = Arrays.toString(fisk).substring(1);
    csv = csv.substring(0, csv.length() - 1);
    String sql = String.format("SELECT %s FROM person p", csv);
    PreparedStatement ps = conn.prepareStatement(sql);
    ps.executeQuery();
    ps.close();
  }

}
