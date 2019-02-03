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

import static com.impossibl.postgres.jdbc.util.Asserts.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CopyTest {

  private Connection con;

  @Before
  public void setup() throws SQLException {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "copytbl", "name text, value int4");
  }

  @After
  public void teardown() throws SQLException {
    TestUtil.dropTable(con, "copytbl");
    TestUtil.closeDB(con);
  }

  @Test
  public void testCopyFromSystemIn() throws SQLException {

    System.setIn(new ByteArrayInputStream("ab\t1\nbc\t20\ncd\t300".getBytes(UTF_8)));

    try (Statement statement = con.createStatement()) {

      statement.executeUpdate("COPY copytbl(name, value) FROM STDIN");

      try (ResultSet rs = statement.executeQuery("SELECT * FROM copytbl")) {

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("ab"));
        assertThat(rs.getInt(2), equalTo(1));

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("bc"));
        assertThat(rs.getInt(2), equalTo(20));

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("cd"));
        assertThat(rs.getInt(2), equalTo(300));

      }
    }

  }

  @Test
  public void testCopyFromSpecifiedIn() throws SQLException {

    InputStream in = new ByteArrayInputStream("ab\t1\nbc\t20\ncd\t300".getBytes(UTF_8));
    con.unwrap(PGConnection.class).copyIn("COPY copytbl FROM STDIN", in);

    try (Statement statement = con.createStatement()) {
      try (ResultSet rs = statement.executeQuery("SELECT * FROM copytbl")) {

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("ab"));
        assertThat(rs.getInt(2), equalTo(1));

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("bc"));
        assertThat(rs.getInt(2), equalTo(20));

        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("cd"));
        assertThat(rs.getInt(2), equalTo(300));
      }
    }

  }

  @Test
  public void testCopyFromSystemOut() throws SQLException, IOException {

    try (Statement statement = con.createStatement()) {

      statement.executeUpdate("INSERT INTO copytbl VALUES ('ab', 1)");
      statement.executeUpdate("INSERT INTO copytbl VALUES ('bc', 20)");
      statement.executeUpdate("INSERT INTO copytbl VALUES ('cd', 300)");

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      System.setOut(new PrintStream(os));

      statement.executeUpdate("COPY copytbl(name, value) TO STDOUT ");

      os.flush();

      assertThat(os.toByteArray(), equalTo("ab\t1\nbc\t20\ncd\t300\n".getBytes(UTF_8)));
    }

  }

  @Test
  public void testCopyFromSpecifiedOut() throws SQLException, IOException {

    try (Statement statement = con.createStatement()) {

      statement.executeUpdate("INSERT INTO copytbl VALUES ('ab', 1)");
      statement.executeUpdate("INSERT INTO copytbl VALUES ('bc', 20)");
      statement.executeUpdate("INSERT INTO copytbl VALUES ('cd', 300)");

      ByteArrayOutputStream os = new ByteArrayOutputStream();

      con.unwrap(PGConnection.class).copyOut("COPY copytbl TO STDOUT", os);

      os.flush();

      assertThat(os.toByteArray(), equalTo("ab\t1\nbc\t20\ncd\t300\n".getBytes(UTF_8)));
    }

  }

  @Test
  public void testCopyInInvalid() {

    assertThrows(SQLException.class, () -> {

      ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);

      con.unwrap(PGConnection.class).copyIn("SELECT * FROM copytbl", is);
    });
  }

  @Test
  public void testCopyOutInvalid() {

    assertThrows(SQLException.class, () -> {

      ByteArrayOutputStream os = new ByteArrayOutputStream();

      con.unwrap(PGConnection.class).copyOut("SELECT * FROM copytbl", os);
    });
  }

}
