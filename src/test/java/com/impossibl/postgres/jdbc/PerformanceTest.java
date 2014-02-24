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

import com.impossibl.postgres.utils.Timer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PerformanceTest {

  static Connection conn;


  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.closeDB(conn);
  }

  @Test
  public void testPreparedStatementCache() throws SQLException {

    long start = System.nanoTime();
    for (int i = 0; i < 10000; i++) {
      PreparedStatement prepareStatement = conn.prepareStatement("SELECT md5(?),md5(?),md5(?),md5(?),md5(?),md5(?)");
      prepareStatement.setString(1, "Some text to hash");
      prepareStatement.setString(2, "Some text to hash");
      prepareStatement.setString(3, "Some text to hash");
      prepareStatement.setString(4, "Some text to hash");
      prepareStatement.setString(5, "Some text to hash");
      prepareStatement.setString(6, "Some text to hash");
      prepareStatement.execute();
      prepareStatement.close();
    }

    System.out.println("Prepare: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
  }

  @Test
  public void testLargeResultSet() throws Exception {

    Timer timer = new Timer();

    for (int c = 0; c < 100; ++c) {

      try (Statement st = conn.createStatement()) {

        try (ResultSet rs = st.executeQuery("SELECT id, md5(random()::text) AS descr FROM (SELECT * FROM generate_series(1,100000) AS id) AS x;")) {

          while (rs.next()) {
            rs.getString(1);
          }

        }

      }

      System.out.println("Query Time:" + timer.getLapSeconds());
    }

  }

}
