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

import com.impossibl.postgres.utils.guava.CharStreams;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertTrue;



/**
 * Checks for when the server disappears without closing the connection
 *
 * NOTE: this cannot be run during normal tests as it nukes the postgres
 * process; which puts the sysetm in recovery mode. If we know how to nuke the
 * process without recovery mode that would be helpful.
 * 
 * @author kdubb
 * 
 */
@RunWith(JUnit4.class)
@Ignore
public class ServerDisconnectTest {

  Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
  }

  @After
  public void tearDown() {
  }

  @Test(expected = SQLException.class)
  public void testServerDisconnect() throws SQLException {

    try (Statement stmt = conn.createStatement()) {

      // Query connection pid
      final int pid;
      try (ResultSet rs = stmt.executeQuery("SELECT pg_backend_pid();")) {

        assertTrue(rs.next());
        pid = rs.getInt(1);
        assertTrue(pid != 0);
      }

      // Kill the postgres process for this connection after 1 second...
      Thread killThread = new Thread(() -> {
        try {
          Thread.sleep(1000);
          Process kill = Runtime.getRuntime().exec("kill -KILL " + pid);

          CharStreams.copy(new InputStreamReader(kill.getErrorStream()), System.err);
        }
        catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      });
      killThread.start();

      stmt.executeQuery("SELECT pg_sleep(3);");

    }

  }


}
