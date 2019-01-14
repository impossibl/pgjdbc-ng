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
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(JUnit4.class)
public class NotificationTest {

  @Test
  public void testExplicitCloseReportsClose() throws Exception {

    AtomicBoolean flag = new AtomicBoolean();
    try (PGConnection connection = TestUtil.openDB().unwrap(PGConnection.class)) {

      connection.addNotificationListener(new PGNotificationListener() {
        @Override
        public void closed() {
          flag.set(true);
        }
      });

    }

    assertTrue(flag.get());

  }

  @Test
  public void testImplicitCloseReportsClose() throws Exception {

    AtomicBoolean flag = new AtomicBoolean();
    PGConnection conn = TestUtil.openDB().unwrap(PGConnection.class);

    conn.addNotificationListener(new PGNotificationListener() {
      @Override
      public void closed() {
        flag.set(true);
      }
    });


    long connPid;
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_backend_pid()")) {
      ResultSet rs = ps.executeQuery();
      rs.next();
      connPid = rs.getLong(1);
      rs.close();
    }

    try (Connection conn2 = TestUtil.openDB()) {
      try (Statement statement = conn2.createStatement()) {
        statement.execute("SELECT pg_terminate_backend(" + connPid + ")");
      }
    }

    Thread.sleep(100);

    assertTrue(flag.get());
    assertTrue(conn.isClosed());
  }

  @Test
  public void testQueryInNotification() throws Exception {

    try (PGConnection conn = TestUtil.openDB().unwrap(PGConnection.class)) {

      final AtomicBoolean flag = new AtomicBoolean(false);
      PGNotificationListener notificationListener = new PGNotificationListener() {

        @Override
        public void notification(int processId, String channelName, String payload) {
          flag.set(true);

          try (Connection conn = TestUtil.openDB()) {
            try (Statement statement = conn.createStatement()) {
              statement.execute("SELECT 1");
            }
          }
          catch (Exception e) {
            fail("Should not fail");
          }
        }

      };

      conn.addNotificationListener(notificationListener);

      try (Statement stmt = conn.createStatement()) {

        stmt.execute("LISTEN TestChannel");
        stmt.execute("NOTIFY TestChannel");

      }

      assertTrue(flag.get());
    }

  }

  @Test
  public void testSimpleNotification() throws Exception {

    try (PGConnection conn = TestUtil.openDB().unwrap(PGConnection.class)) {

      final AtomicBoolean flag = new AtomicBoolean(false);
      PGNotificationListener notificationListener = new PGNotificationListener() {

        @Override
        public void notification(int processId, String channelName, String payload) {
          flag.set(true);

        }

      };

      conn.addNotificationListener(notificationListener);

      try (Statement stmt = conn.createStatement()) {

        stmt.execute("LISTEN TestChannel");
        stmt.execute("NOTIFY TestChannel");

      }

      assertTrue(flag.get());
    }

  }

  @Test
  public void testFilteredNotification() throws Exception {

    try (PGConnection conn = TestUtil.openDB().unwrap(PGConnection.class)) {

      final AtomicBoolean validFlag = new AtomicBoolean(false);
      PGNotificationListener validNotificationListener = new PGNotificationListener() {

        @Override
        public void notification(int processId, String channelName, String payload) {
          validFlag.set(true);
        }

      };
      conn.addNotificationListener("1.*", validNotificationListener);

      final AtomicBoolean invalidFlag = new AtomicBoolean(false);
      PGNotificationListener invalidNotificationListener = new PGNotificationListener() {

        @Override
        public void notification(int processId, String channelName, String payload) {
          invalidFlag.set(true);
        }

      };
      conn.addNotificationListener("2.*", invalidNotificationListener);

      final AtomicBoolean allFlag = new AtomicBoolean(false);
      PGNotificationListener allNotificationListener = new PGNotificationListener() {

        @Override
        public void notification(int processId, String channelName, String payload) {
          allFlag.set(true);
        }

      };
      conn.addNotificationListener(allNotificationListener);

      try (Statement stmt = conn.createStatement()) {

        stmt.execute("LISTEN \"1channel\"");
        stmt.execute("LISTEN \"2channel\"");
        stmt.execute("NOTIFY \"1channel\"");

      }

      assertTrue(validFlag.get());
      assertFalse(invalidFlag.get());
      assertTrue(allFlag.get());
    }

  }

}
