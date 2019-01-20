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

import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.Path;
import com.impossibl.postgres.jdbc.util.BrokenInputStream;
import com.impossibl.postgres.jdbc.util.BrokenReader;
import com.impossibl.postgres.system.procs.Bytes;
import com.impossibl.postgres.test.annotations.ConnectionSetting;
import com.impossibl.postgres.test.annotations.DBTest;
import com.impossibl.postgres.test.annotations.ExtensionInstalled;
import com.impossibl.postgres.test.annotations.Prepare;
import com.impossibl.postgres.test.annotations.Random;
import com.impossibl.postgres.test.annotations.Table;
import com.impossibl.postgres.test.matchers.ColSnapshot;
import com.impossibl.postgres.test.matchers.RowSnapshot;
import com.impossibl.postgres.utils.GeometryParsers;
import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.test.matchers.InputStreamMatcher.contentEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static com.google.common.collect.Streams.zip;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@DBTest
class PreparedStatementTest {

  @DisplayName("Geometrics")
  @Nested
  class GeometricTests {

    @DisplayName("Points")
    @Test
    @Table(name = "pointtable", columns = {"p1 point", "p2 point", "p3 point"})
    @Prepare(name = "set", sql = "INSERT INTO pointtable VALUES (?, ?, ?)")
    void testPoint(PreparedStatement set, RowSnapshot<double[]> pointtable) throws SQLException {
      double[] p1 = new double[] {45.0, 56.3};
      double[] p2 = new double[] {0, 0};
      set.setObject(1, p1);
      set.setObject(2, p2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(pointtable.take(), hasItem(new double[][] {p1, p2, null}));
    }

    @DisplayName("Paths")
    @Test
    @Table(name = "pathtable", columns = {"p1 path", "p2 path", "p3 path"})
    @Prepare(name = "set", sql = "INSERT INTO pathtable VALUES (?, ?, ?)")
    void testPath(PreparedStatement set, RowSnapshot<Path> pathtable) throws SQLException {
      Path p1 = GeometryParsers.INSTANCE.parsePath("[(678.6,454),(10,89),(124.6,0)]");
      Path p2 = GeometryParsers.INSTANCE.parsePath("((678.6,454),(10,89),(124.6,0))");
      set.setObject(1, p1);
      set.setObject(2, p2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(pathtable.take(), hasItem(new Path[] {p1, p2, null}));
    }

    @DisplayName("Polygons")
    @Test
    @Table(name = "polytable", columns = {"p1 polygon", "p2 polygon", "p3 polygon"})
    @Prepare(name = "set", sql = "INSERT INTO polytable VALUES (?, ?, ?)")
    void testPolygon(PreparedStatement set, RowSnapshot<double[][]> polytable) throws SQLException {
      double[][] p1 = GeometryParsers.INSTANCE.parsePolygon("((678.6,454),(10,89),(124.6,0),(0,0))");
      double[][] p2 = GeometryParsers.INSTANCE.parsePolygon("((678.6,454),(10,89),(124.6,0))");
      set.setObject(1, p1);
      set.setObject(2, p2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(polytable.take(), hasItem(new double[][][] {p1, p2, null}));
    }

    @DisplayName("Circles")
    @Test
    @Table(name = "circletable", columns = {"p1 circle", "p2 circle", "p3 circle"})
    @Prepare(name = "set", sql = "INSERT INTO circletable VALUES (?, ?, ?)")
    void testCircle(PreparedStatement set, RowSnapshot<double[]> circletable) throws SQLException {
      double[] c1 = new double[] {45.0, 56.3, 40};
      double[] c2 = new double[] {0, 0, 0};
      set.setObject(1, c1);
      set.setObject(2, c2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(circletable.take(), hasItem(new double[][] {c1, c2, null}));
    }

    @DisplayName("LSegs")
    @Test
    @Table(name = "lsegtable", columns = {"p1 lseg", "p2 lseg", "p3 lseg"})
    @Prepare(name = "set", sql = "INSERT INTO lsegtable VALUES (?, ?, ?)")
    void testLSeg(PreparedStatement set, RowSnapshot<double[]> lsegtable) throws SQLException {
      double[] l1 = new double[] {45.0, 60.0, 40.9, 56.3};
      double[] l2 = new double[] {0, 0, 0, 0};
      set.setObject(1, l1);
      set.setObject(2, l2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(lsegtable.take(), hasItem(new double[][] {l1, l2, null}));
    }

    @DisplayName("Boxes")
    @Test
    @Table(name = "boxtable", columns = {"p1 box", "p2 box", "p3 box"})
    @Prepare(name = "set", sql = "INSERT INTO boxtable VALUES (?, ?, ?)")
    void testBox(PreparedStatement set, RowSnapshot<double[]> boxtable) throws SQLException {
      double[] b1 = new double[] {45.0, 60.0, 40.9, 56.3};
      double[] b2 = new double[] {0, 0, 0, 0};
      set.setObject(1, b1);
      set.setObject(2, b2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(boxtable.take(), hasItem(new double[][] {b1, b2, null}));
    }

  }

  @DisplayName("HStores")
  @Table(name = "hstable", columns = {"hs1 hstore"})
  @Prepare(name = "set", sql = "INSERT INTO hstable VALUES (?)")
  @Test
  @ExtensionInstalled("hstore")
  void testHStore(PreparedStatement set, ColSnapshot<Map<String, String>> hstable) throws SQLException {

    Map<String, String> hs1 = new HashMap<>();
    hs1.put("k1", "v1");
    hs1.put("k2", "v2");
    hs1.put("k3", "v3");
    hs1.put("k4", "v4");

    Map<String, String> hs2 = new HashMap<>();

    set.setObject(1, hs1);
    set.executeUpdate();
    set.setObject(1, hs2);
    set.executeUpdate();
    set.setObject(1, null, Types.OTHER);
    set.executeUpdate();

    Collection<Map<String, String>> data = hstable.take();
    assertThat(data, hasItem(hs1));
    assertThat(data, hasItem(hs2));
    assertThat(data, hasItem(nullValue()));
  }

  @DisplayName("Networks")
  @Nested
  class NetworkTests {

    @DisplayName("inet")
    @Table(name = "inettable", columns = {"ip1 inet", "ip2 inet", "ip3 inet"})
    @Prepare(name = "set", sql = "INSERT INTO inettable VALUES (?, ?, ?)")
    @Test
    void testInet(PreparedStatement set, RowSnapshot<InetAddr> inettable) throws SQLException {
      InetAddr inet1 = new InetAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1");
      InetAddr inet2 = new InetAddr("192.168.100.128/25");
      set.setObject(1, inet1);
      set.setObject(2, inet2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(inettable.take(), hasItem(new InetAddr[] {inet1, inet2, null}));
    }

    @DisplayName("cidr")
    @Table(name = "cidrtable", columns = {"ip1 cidr", "ip2 cidr", "ip3 cidr"})
    @Prepare(name = "set", sql = "INSERT INTO cidrtable VALUES (?, ?, ?)")
    @Test
    void testCidr(PreparedStatement set, RowSnapshot<CidrAddr> cidrtable) throws SQLException {
      CidrAddr cidr1 = new CidrAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1");
      CidrAddr cidr2 = new CidrAddr("2001:4f8:3:ba::/64");
      set.setObject(1, cidr1);
      set.setObject(2, cidr2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(cidrtable.take(), hasItem(new CidrAddr[] {cidr1, cidr2, null}));
    }

    @DisplayName("mac")
    @Table(name = "mactable", columns = {"mac1 macaddr", "mac2 macaddr", "mac3 macaddr"})
    @Prepare(name = "set", sql = "INSERT INTO mactable VALUES (?, ?, ?)")
    @Test
    void testMac(PreparedStatement set, RowSnapshot<byte[]> mactable) throws SQLException {
      byte[] mac1 = new byte[] {0x08, 0x00, 0x2b, 0x01, 0x02, 0x03};
      byte[] mac2 = new byte[] {0x08, 0x4f, 0x2a, 0x01, 0x02, 0x3e};
      set.setObject(1, mac1);
      set.setObject(2, mac2);
      set.setObject(3, null, Types.OTHER);
      set.executeUpdate();

      assertThat(mactable.take(), hasItem(new byte[][] {mac1, mac2, null}));
    }

  }

  @DisplayName("Set RowId")
  @Table(name = "rowidtable", columns = "val text")
  @Prepare(name = "insert", sql = "INSERT INTO rowidtable (val) VALUES (?)", returning = {"ctid"})
  @Prepare(name = "select", sql = "SELECT val FROM rowidtable WHERE ctid = ?")
  @Test
  void testRowId(PreparedStatement insert, PreparedStatement select) throws SQLException {
    insert.setString(1, "some text");
    insert.executeUpdate();
    try (ResultSet keys = insert.getGeneratedKeys()) {
      assertTrue(keys.next());
      RowId rowId = keys.getRowId(1);

      select.setRowId(1, rowId);
      try (ResultSet vals = select.executeQuery()) {
        assertThat(vals.next(), equalTo(true));
        assertThat(vals.getString(1), equalTo("some text"));
      }
    }
  }

  @DisplayName("Nulls are set correctly")
  @Table(name = "nulltable", columns = "val text")
  @Prepare(name = "set", sql = "INSERT INTO nulltable VALUES (?)")
  @Test
  void testSetNull(PreparedStatement set, ColSnapshot<String> nulltable) throws SQLException {
    // valid: fully qualified type to setNull()
    set.setNull(1, Types.VARCHAR);
    set.executeUpdate();
    assertThat(nulltable.take(), hasItem(nullValue()));
    // valid: fully qualified type to setObject()
    set.setObject(1, null, Types.VARCHAR);
    set.executeUpdate();
    assertThat(nulltable.take(), hasItem(nullValue()));
    // setObject() with no type info
    set.setObject(1, null);
    set.executeUpdate();
    assertThat(nulltable.take(), hasItem(nullValue()));
    // setObject() with insufficient type info
    set.setObject(1, null, Types.OTHER);
    set.executeUpdate();
    assertThat(nulltable.take(), hasItem(nullValue()));
    // setNull() with insufficient type info
    set.setNull(1, Types.OTHER);
    set.executeUpdate();
    assertThat(nulltable.take(), hasItem(nullValue()));
  }

  @DisplayName("Double quote string parameters")
  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {
          "bare ? question mark", "single ' quote", "doubled '' single quote",
          "doubled \"\" double quote", "no backslash interpretation here: \\"
      }
  )
  void testDoubleQuotes(String value, Statement statement) throws SQLException {
    statement.executeUpdate("CREATE TEMP TABLE \"" + value + "\" (i integer)");
    statement.executeUpdate("DROP TABLE \"" + value + "\"");
  }

  @DisplayName("Dollar quotes are parsed correctly")
  @ParameterizedTest()
  @CsvSource(
      delimiter = '|',
      value = {
          "SELECT $$;$$ WHERE $x$?$x$=$_0$?$_0$ AND $$?$$=?|?|;",
          "SELECT $__$;$__$ WHERE ''''=$q_1$'$q_1$ AND ';'=?;|;|;",
          "SELECT $x$$a$;$x $a$$x$ WHERE $$;$$=? OR ''=$c$c$;$c$;|;|$a$;$x $a$",
          "SELECT ?::text|$a$ $a$|$a$ $a$"
      }
  )
  void testDollarQuotes(String query, String value, String expected, Connection conn) throws SQLException {
    try (PreparedStatement st = conn.prepareStatement(query)) {
      st.setString(1, value);
      try (ResultSet rs = st.executeQuery()) {
        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo(expected));
        assertThat(rs.next(), equalTo(false));
      }
    }
  }

  @DisplayName("Dollar quotes in identifiers are parsed correctly")
  @Test
  void testDollarQuotesAndIdentifiers(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE TEMP TABLE a$b$c(a varchar, b varchar)");
    }

    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO a$b$c (a, b) VALUES (?, ?)")) {
      ps.setString(1, "a");
      ps.setString(2, "b");
      ps.executeUpdate();
    }

    try (Statement st = conn.createStatement()) {
      st.execute("CREATE TEMP TABLE e$f$g(h varchar, e$f$g varchar) ");
    }

    try (PreparedStatement ps = conn.prepareStatement("UPDATE e$f$g SET h = ? || e$f$g")) {
      ps.setString(1, "a");
      ps.executeUpdate();
    }

  }

  @DisplayName("Comments are parsed correctly")
  @Test
  void testComments(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement()) {
      assertThat(st.execute("SELECT /*?*/ /*/*/*/**/*/*/*/1;SELECT 1;--SELECT 1"), equalTo(true));
      assertThat(st.getMoreResults(), equalTo(true));
      assertThat(st.getMoreResults(), equalTo(false));
    }

    try (PreparedStatement pst = conn.prepareStatement("SELECT /**/'?'/*/**/*/ WHERE '?'=/*/*/*?*/*/*/--?\n?")) {
      pst.setString(1, "?");
      try (ResultSet rs = pst.executeQuery()) {
        assertThat(rs.next(), equalTo(true));
        assertThat(rs.getString(1), equalTo("?"));
        assertThat(rs.next(), equalTo(false));
      }
    }
  }

  @DisplayName("Single quote string parameters")
  @TestInstance(PER_CLASS)
  @Nested
  class SingleQuoteStringParameterTests {

    Stream<Arguments> nonStdStringArgs() {
      String[] strings = new String[] {
          "bare ? question mark", "quoted \\' single quote", "doubled '' single quote", "octal \\060 constant",
          "escaped \\? question mark", "double \\\\ backslash", "double \" quote", "backslash \\\\\\' single quote"
      };
      String[] expected = new String[] {
          "bare ? question mark", "quoted ' single quote", "doubled ' single quote", "octal 0 constant",
          "escaped ? question mark", "double \\ backslash", "double \" quote", "backslash \\' single quote"
      };
      return zip(Stream.of(strings), Stream.of(expected), Arguments::arguments);
    }

    @DisplayName("Test with non-std strings & setting off")
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonStdStringArgs")
    @ConnectionSetting(name = "standard_conforming_strings", value = "off")
    void testNonStdStringsStdOff(String value, String expected, Connection connection) throws SQLException {
      try (PreparedStatement ps = connection.prepareStatement("SELECT '" + value + "'")) {
        try (ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString(1), equalTo(expected));
        }
      }
    }

    @DisplayName("Test with escaped non-std strings & setting on")
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonStdStringArgs")
    @ConnectionSetting(name = "standard_conforming_strings", value = "on")
    void testNonStdStringsStdOn(String value, String expected, Connection connection) throws SQLException {
      try (PreparedStatement ps = connection.prepareStatement("SELECT E'" + value + "'")) {
        try (ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString(1), equalTo(expected));
        }
      }
    }

    Stream<Arguments> stdStringArgs() {
      String[] strings = new String[] {
          "bare ? question mark", "quoted '' single quote", "doubled '' single quote", "octal 0 constant",
          "escaped ? question mark", "double \\ backslash", "double \" quote", "backslash \\'' single quote"
      };
      String[] expected = new String[] {
          "bare ? question mark", "quoted ' single quote", "doubled ' single quote", "octal 0 constant",
          "escaped ? question mark", "double \\ backslash", "double \" quote", "backslash \\' single quote"
      };
      return zip(Stream.of(strings), Stream.of(expected), Arguments::arguments);
    }

    @DisplayName("Test with std strings & setting on")
    @ParameterizedTest(name = "{0}")
    @MethodSource("stdStringArgs")
    @ConnectionSetting(name = "standard_conforming_strings", value = "on")
    void testStdStringsStdOn(String value, String expected, Connection connection) throws SQLException {
      try (PreparedStatement ps = connection.prepareStatement("SELECT '" + value + "'")) {
        try (ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next(), equalTo(true));
          assertThat(rs.getString(1), equalTo(expected));
        }
      }
    }

  }

  @DisplayName("Test leading/trailing spaces respected in text types")
  @Table(name = "texttable", columns = {"ch char(3)", "te text", "vc varchar"})
  @Prepare(name = "ps", sql = "INSERT INTO texttable(ch, te, vc) VALUES (?, ?, ?)")
  @Test
  void testSpaces(PreparedStatement ps, RowSnapshot<String> texttable) throws SQLException {
    String str = " a ";
    ps.setString(1, str);
    ps.setString(2, str);
    ps.setString(3, str);
    ps.executeUpdate();

    List<String[]> data = texttable.take();
    assertThat(data.size(), equalTo(1));
    assertThat(data.get(0), equalTo(new String[] {str, str, str}));
  }

  @DisplayName("Test character can be set into text")
  @Table(name = "texttable", columns = {"te text"})
  @Prepare(name = "ps", sql = "INSERT INTO texttable(te) VALUES (?)")
  @Test
  void testChar(PreparedStatement ps, ColSnapshot<String> texttable) throws SQLException {
    ps.setObject(1, 'z');
    ps.executeUpdate();

    List<String> data = texttable.take();
    assertThat(data.size(), equalTo(1));
    assertThat(data.get(0), equalTo("z"));
  }

  @DisplayName("Setting Parameters")
  @Table(name = "paramtable", columns = {"val text", "val2 text"})
  @Prepare(name = "set", sql = "INSERT INTO paramtable VALUES (?, ?)")
  @Nested
  class ParamTests {

    @DisplayName("Executes fail when missing all parameters")
    @Test
    void test1(PreparedStatement set) {
      assertThrows(SQLException.class, set::execute);
      assertThrows(SQLException.class, set::executeQuery);
      assertThrows(SQLException.class, set::executeUpdate);
    }

    @DisplayName("Executes fail when missing some parameters")
    @Test
    void test2(PreparedStatement set) throws SQLException {
      set.setObject(1, "hello");
      assertThrows(SQLException.class, set::execute);
      assertThrows(SQLException.class, set::executeQuery);
      assertThrows(SQLException.class, set::executeUpdate);
    }

    @DisplayName("Executes fail after clearing parameters")
    @Test
    void test3(PreparedStatement set) throws SQLException {
      set.setObject(1, "hello");
      set.setObject(2, "world");
      set.executeUpdate();

      set.clearParameters();
      assertThrows(SQLException.class, set::execute);
      assertThrows(SQLException.class, set::executeQuery);
      assertThrows(SQLException.class, set::executeUpdate);
    }

  }

  @DisplayName("Invoke invalid methods inherited from Statement")
  @Prepare(name = "ps", sql = "SELECT 1")
  @Test
  void testInvokeInvalid(PreparedStatement ps) {
    assertThrows(SQLException.class, () -> ps.execute("SELECT 1"));
    assertThrows(SQLException.class, () -> ps.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLException.class, () -> ps.execute("SELECT 1", new String[] {"oid"}));
    assertThrows(SQLException.class, () -> ps.executeQuery("SELECT 1"));
    assertThrows(SQLException.class, () -> ps.executeUpdate("SELECT 1"));
    assertThrows(SQLException.class, () -> ps.executeUpdate("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLException.class, () -> ps.executeUpdate("SELECT 1", new String[] {"oid"}));
    assertThrows(SQLException.class, () -> ps.executeLargeUpdate("SELECT 1"));
    assertThrows(SQLException.class, () -> ps.executeLargeUpdate("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLException.class, () -> ps.executeLargeUpdate("SELECT 1", new String[] {"oid"}));
  }

  @DisplayName("Binary Values")
  @Table(name = "bintable", columns = {"val bytea"})
  @Prepare(name = "set", sql = "INSERT INTO bintable VALUES (?)")
  @Nested
  class BinaryTests {

    @DisplayName("Set as BINARY")
    @Test
    void test1(PreparedStatement set, @Random byte[] bytes, ColSnapshot<InputStream> bintable) throws SQLException {
      set.setObject(1, bytes, Types.BINARY);
      set.executeUpdate();

      List<InputStream> data = bintable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(new ByteArrayInputStream(bytes)));

      List<String> text = bintable.takeText();
      assertThat(text.size(), equalTo(1));
      assertThat(text.get(0), equalTo("\\x" + Bytes.encodeHex(bytes).toLowerCase()));

    }

    @DisplayName("Set as VARBINARY")
    @Test
    void test2(PreparedStatement set, @Random byte[] bytes, ColSnapshot<InputStream> bintable) throws SQLException {
      set.setObject(1, bytes, Types.VARBINARY);
      set.executeUpdate();

      List<InputStream> data = bintable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(new ByteArrayInputStream(bytes)));

      List<String> text = bintable.takeText();
      assertThat(text.size(), equalTo(1));
      assertThat(text.get(0), equalTo("\\x" + Bytes.encodeHex(bytes).toLowerCase()));
    }

    @DisplayName("Set as LONGVARBINARY")
    @Test
    void test3(PreparedStatement set, @Random byte[] bytes, ColSnapshot<InputStream> bintable) throws SQLException {
      set.setObject(1, bytes, Types.LONGVARBINARY);
      set.executeUpdate();

      List<InputStream> data = bintable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(new ByteArrayInputStream(bytes)));

      List<String> text = bintable.takeText();
      assertThat(text.size(), equalTo(1));
      assertThat(text.get(0), equalTo("\\x" + Bytes.encodeHex(bytes).toLowerCase()));
    }

  }

  @DisplayName("Integers")
  @Table(name = "inttable", columns = {"val int4"})
  @Prepare(name = "set", sql = "INSERT INTO inttable VALUES (?)")
  @Nested
  class IntegerTests {

    @DisplayName("Set null")
    @Test
    void test0(PreparedStatement set, ColSnapshot<Integer> inttable) throws SQLException {
      set.setNull(1, Types.INTEGER);
      set.executeUpdate();

      Collection<Integer> data = inttable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set")
    @ParameterizedTest
    @ValueSource(ints = {Integer.MAX_VALUE, Integer.MIN_VALUE})
    void test1(int value, PreparedStatement set, ColSnapshot<Integer> inttable) throws SQLException {
      set.setInt(1, value);
      set.executeUpdate();

      Collection<Integer> data = inttable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(value));
    }

    @DisplayName("Set from Float")
    @Test
    void test2(PreparedStatement set, ColSnapshot<Integer> inttable) throws SQLException {
      set.setObject(1, 1000f, Types.INTEGER);
      set.executeUpdate();
      set.setObject(1, -1000f, Types.INTEGER);
      set.executeUpdate();

      Collection<Integer> data = inttable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(1000, -1000));
    }

    @DisplayName("Set from TINYINT")
    @Test
    void test3(PreparedStatement set, ColSnapshot<Integer> inttable) throws SQLException {
      set.setObject(1, 127, Types.TINYINT);
      set.executeUpdate();
      set.setObject(1, -128, Types.TINYINT);
      set.executeUpdate();

      Collection<Integer> data = inttable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(127, -128));
    }

    @DisplayName("Set from SMALLINT")
    @Test
    void test4(PreparedStatement set, ColSnapshot<Integer> inttable) throws SQLException {
      set.setObject(1, 32767, Types.SMALLINT);
      set.executeUpdate();
      set.setObject(1, -32768, Types.SMALLINT);
      set.executeUpdate();

      Collection<Integer> data = inttable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(32767, -32768));
    }

  }

  @DisplayName("Numerics")
  @Table(name = "numtable", columns = {"val numeric(30,15)"})
  @Prepare(name = "set", sql = "INSERT INTO numtable VALUES (?)")
  @Nested
  class NumericTests {

    @DisplayName("Set")
    @Test
    void test1(PreparedStatement set, ColSnapshot<BigDecimal> numtable) throws SQLException {
      BigDecimal bigDecimal1 = new BigDecimal("123456789.987654321000000");
      BigDecimal bigDecimal2 = new BigDecimal("-123456789.987654321000000");
      set.setBigDecimal(1, bigDecimal1);
      set.executeUpdate();
      set.setBigDecimal(1, bigDecimal2);
      set.executeUpdate();
      set.setNull(1, Types.NUMERIC);
      set.executeUpdate();

      Collection<BigDecimal> data = numtable.take();
      assertThat(data.size(), equalTo(3));
      assertThat(data, hasItems(bigDecimal1, bigDecimal2));
      assertThat(data, hasItem(nullValue()));

      Collection<String> text = numtable.takeText();
      assertThat(text.size(), equalTo(3));
      assertThat(text, hasItems("123456789.987654321000000", "-123456789.987654321000000"));
    }

    @DisplayName("Set from Boolean (as NUMERIC)")
    @Test
    void test2(PreparedStatement set, ColSnapshot<BigDecimal> numtable) throws SQLException {
      set.setObject(1, true, Types.NUMERIC);
      set.executeUpdate();
      set.setObject(1, false, Types.NUMERIC);
      set.executeUpdate();

      Collection<BigDecimal> data = numtable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(new BigDecimal("1.000000000000000"), new BigDecimal("0.000000000000000")));

      Collection<String> text = numtable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1.000000000000000", "0.000000000000000"));
    }

    @DisplayName("Set from Boolean (as DECIMAL)")
    @Test
    void test3(PreparedStatement set, ColSnapshot<BigDecimal> numtable) throws SQLException {
      set.setObject(1, true, Types.DECIMAL);
      set.executeUpdate();
      set.setObject(1, false, Types.DECIMAL);
      set.executeUpdate();

      Collection<BigDecimal> data = numtable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(new BigDecimal("1.000000000000000"), new BigDecimal("0.000000000000000")));

      Collection<String> text = numtable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1.000000000000000", "0.000000000000000"));
    }

  }

  @DisplayName("Doubles")
  @Table(name = "doubletable", columns = {"val float"})
  @Prepare(name = "set", sql = "INSERT INTO doubletable VALUES (?)")
  @Nested
  class DoubleTests {

    @DisplayName("Set")
    @Test
    void test1(PreparedStatement set, ColSnapshot<Double> doubletable) throws Exception {
      set.setDouble(1, 1.0E125);
      set.executeUpdate();
      set.setDouble(1, 1.0E-130);
      set.executeUpdate();
      set.setNull(1, Types.DOUBLE);
      set.executeUpdate();

      Collection<Double> data = doubletable.take();
      assertThat(data.size(), equalTo(3));
      assertThat(data, hasItems(1.0E125, 1.0E-130));
      assertThat(data, hasItem(nullValue()));

      Collection<String> text = doubletable.takeText();
      assertThat(text.size(), equalTo(3));
      assertThat(text, hasItems("1e+125", "1e-130"));
      assertThat(text, hasItem(nullValue()));
    }

    @DisplayName("Set from Boolean")
    @Test
    void test2(PreparedStatement set, ColSnapshot<Double> doubletable) throws Exception {
      set.setObject(1, true, Types.DOUBLE);
      set.executeUpdate();
      set.setObject(1, false, Types.DOUBLE);
      set.executeUpdate();

      Collection<Double> data = doubletable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(1d, 0d));

      Collection<String> text = doubletable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1", "0"));
    }

    @DisplayName("Set from Integer")
    @Test
    void test3(PreparedStatement set, ColSnapshot<Double> doubletable) throws SQLException {
      Integer maxInteger = Integer.MAX_VALUE, minInteger = Integer.MIN_VALUE;
      Double maxFloat = (double) maxInteger, minFloat = (double) minInteger;
      set.setObject(1, maxInteger, Types.DOUBLE);
      set.executeUpdate();
      set.setObject(1, minInteger, Types.DOUBLE);
      set.executeUpdate();

      Collection<Double> data = doubletable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = doubletable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("2147483647", "-2147483648"));
    }

    @DisplayName("Set from String")
    @Test
    void test4(PreparedStatement set, ColSnapshot<Double> doubletable) throws SQLException {
      String maxString = "1.0E125", minString = "1.0E-130";
      Double maxFloat = Double.valueOf(maxString), minFloat = Double.valueOf(minString);
      set.setObject(1, maxString, Types.DOUBLE);
      set.executeUpdate();
      set.setObject(1, minString, Types.DOUBLE);
      set.executeUpdate();

      Collection<Double> data = doubletable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = doubletable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1e+125", "1e-130"));
    }

    @DisplayName("Set from BigDecimal")
    @Test
    void test5(PreparedStatement set, ColSnapshot<Double> doubletable) throws SQLException {
      BigDecimal maxDecimal = new BigDecimal("1.0E125"), minDecimal = new BigDecimal("1.0E-130");
      Double maxFloat = maxDecimal.doubleValue(), minFloat = maxDecimal.doubleValue();
      set.setObject(1, maxDecimal, Types.DOUBLE);
      set.executeUpdate();
      set.setObject(1, minDecimal, Types.DOUBLE);
      set.executeUpdate();

      Collection<Double> data = doubletable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = doubletable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1e+125", "1e-130"));
    }

  }

  @DisplayName("Floats")
  @Table(name = "floattable", columns = {"val real"})
  @Prepare(name = "set", sql = "INSERT INTO floattable VALUES (?)")
  @Nested
  class FloatTests {

    @DisplayName("Set")
    @Test
    void test1(PreparedStatement set, ColSnapshot<Float> floattable) throws Exception {
      set.setFloat(1, 1.0E37f);
      set.executeUpdate();
      set.setFloat(1, 1.0E-37f);
      set.executeUpdate();
      set.setNull(1, Types.FLOAT);
      set.executeUpdate();

      Collection<Float> data = floattable.take();
      assertThat(data.size(), equalTo(3));
      assertThat(data, hasItems(1.0E37f, 1.0E-37f));
      assertThat(data, hasItem(nullValue()));

      Collection<String> text = floattable.takeText();
      assertThat(text.size(), equalTo(3));
      assertThat(text, hasItems("1e+37", "1e-37"));
    }

    @DisplayName("Set from Boolean")
    @Test
    void test2(PreparedStatement set, ColSnapshot<Float> floattable) throws Exception {
      set.setObject(1, true, Types.DOUBLE);
      set.executeUpdate();
      set.setObject(1, false, Types.DOUBLE);
      set.executeUpdate();

      Collection<Float> data = floattable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(1f, 0f));

      Collection<String> text = floattable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1", "0"));
    }

    @DisplayName("Set from Integer")
    @Test
    void test3(PreparedStatement set, ColSnapshot<Float> floattable) throws SQLException {
      Integer maxInteger = Integer.MAX_VALUE, minInteger = Integer.MIN_VALUE;
      Float maxFloat = 2.14748006E9F, minFloat = -2.14748006E9F;
      set.setObject(1, maxInteger, Types.FLOAT);
      set.executeUpdate();
      set.setObject(1, minInteger, Types.FLOAT);
      set.executeUpdate();

      Collection<Float> data = floattable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = floattable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("2.14748e+09", "-2.14748e+09"));
    }

    @DisplayName("Set from String")
    @Test
    void test4(PreparedStatement set, ColSnapshot<Float> floattable) throws SQLException {
      String maxString = "1.0E37", minString = "1.0E-37";
      Float maxFloat = Float.valueOf(maxString), minFloat = Float.valueOf(minString);
      set.setObject(1, maxString, Types.FLOAT);
      set.executeUpdate();
      set.setObject(1, minString, Types.FLOAT);
      set.executeUpdate();

      Collection<Float> data = floattable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = floattable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1e+37", "1e-37"));
    }

    @DisplayName("Set from BigDecimal")
    @Test
    void test5(PreparedStatement set, ColSnapshot<Float> floattable) throws SQLException {
      BigDecimal maxDecimal = new BigDecimal("1.0E37"), minDecimal = new BigDecimal("1.0E-37");
      Float maxFloat = maxDecimal.floatValue(), minFloat = maxDecimal.floatValue();
      set.setObject(1, maxDecimal, Types.FLOAT);
      set.executeUpdate();
      set.setObject(1, minDecimal, Types.FLOAT);
      set.executeUpdate();

      Collection<Float> data = floattable.take();
      assertThat(data.size(), equalTo(2));
      assertThat(data, hasItems(maxFloat, minFloat));

      Collection<String> text = floattable.takeText();
      assertThat(text.size(), equalTo(2));
      assertThat(text, hasItems("1e+37", "1e-37"));
    }

  }

  @DisplayName("Set Boolean")
  @Table(name = "booltable", columns = {"val boolean"})
  @Prepare(name = "set", sql = "INSERT INTO booltable VALUES (?)")
  @Test
  void testSetBoolean(PreparedStatement set, ColSnapshot<Boolean> booltable) throws Exception {
    set.setBoolean(1, true);
    set.executeUpdate();
    set.setBoolean(1, false);
    set.executeUpdate();
    set.setNull(1, Types.BIT);
    set.executeUpdate();

    Collection<Boolean> data = booltable.take();
    assertThat(data.size(), equalTo(3));
    assertThat(data, hasItem(true));
    assertThat(data, hasItem(false));
    assertThat(data, hasItem(nullValue()));
  }

  @DisplayName("Binary Streams")
  @Table(name = "streamtable", columns = {"bin bytea"})
  @Prepare(name = "set", sql = "INSERT INTO streamtable (bin) VALUES (?)")
  @Nested
  class BinaryStreams {

    @DisplayName("Set null")
    @Test
    void test1(PreparedStatement set, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, null);
      set.executeUpdate();
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set null")
    @Test
    void test1l(PreparedStatement set, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, null, 0);
      set.executeUpdate();
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set empty")
    @Test
    void test2(PreparedStatement set, @Random(size = 0) InputStream stream, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, stream);
      set.executeUpdate();
      stream.reset();
      List<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(stream));
    }

    @DisplayName("Set non-empty")
    @Test
    void test3(PreparedStatement set, @Random(size = 10) InputStream stream, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, stream);
      set.executeUpdate();
      stream.reset();
      List<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(stream));
    }

    @DisplayName("Set non-empty, capped to zero")
    @Test
    void test4(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, stream, 0);
      set.executeUpdate();
      stream = ByteStreams.limit(stream, 0);
      List<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(stream));
    }

    @DisplayName("Set non-empty, capped by some value")
    @Test
    void test5(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, stream, 5);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 5);
      List<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(stream));
    }

    @DisplayName("Set non-empty, capped by negative length")
    @Test
    void test6(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) {
      assertThrows(SQLException.class, () -> set.setBinaryStream(1, stream, -3));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException")
    @Test
    void test7(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) {
      assertThrows(SQLException.class, () -> set.setBinaryStream(1, new BrokenInputStream(stream, 3)));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException, length specified")
    @Test
    void test8(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) {
      assertThrows(SQLException.class, () -> set.setBinaryStream(1, new BrokenInputStream(stream, 3), 10));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, capped before throws IOException")
    @Test
    void test9(PreparedStatement set, @Random InputStream stream, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setBinaryStream(1, new BrokenInputStream(stream, 3), 1);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 1);
      List<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), contentEquals(stream));
    }

  }

  @DisplayName("ASCII Streams")
  @Table(name = "streamtable", columns = {"str text"})
  @Prepare(name = "set", sql = "INSERT INTO streamtable (str) VALUES (?)")
  @Nested
  class AsciiStreams {

    @DisplayName("Set null")
    @Test
    void test1(PreparedStatement set, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, null);
      set.executeUpdate();
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set null, length specified")
    @Test
    void test1l(PreparedStatement set, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, null, 0);
      set.executeUpdate();
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set empty")
    @Test
    void test2(PreparedStatement set, @Random(size = 0, origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, stream);
      set.executeUpdate();
      stream.reset();
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty")
    @Test
    void test3(PreparedStatement set, @Random(size = 10, origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, stream);
      set.executeUpdate();
      stream.reset();
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped to zero")
    @Test
    void test4(PreparedStatement set, @Random(origin = 1, bound = 128)  InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, stream, 0);
      set.executeUpdate();
      stream = ByteStreams.limit(stream, 0);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by some value")
    @Test
    void test5(PreparedStatement set, @Random(origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, stream, 5);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 5);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by negative length")
    @Test
    void test6(PreparedStatement set, @Random(origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setAsciiStream(1, stream, -3));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException")
    @Test
    void test7(PreparedStatement set, @Random(origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setAsciiStream(1, new BrokenInputStream(stream, 3)));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException, length specified")
    @Test
    void test7b(PreparedStatement set, @Random(origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setAsciiStream(1, new BrokenInputStream(stream, 3), 10));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, capped before throws IOException")
    @Test
    void test7c(PreparedStatement set, @Random(origin = 1, bound = 128) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setAsciiStream(1, new BrokenInputStream(stream, 3), 1);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 1);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), equalTo(txt));
    }

  }

  @DisplayName("Unicode Streams")
  @Table(name = "streamtable", columns = {"str text"})
  @Prepare(name = "set", sql = "INSERT INTO streamtable (str) VALUES (?)")
  @Nested
  @SuppressWarnings("deprecation")
  class UnicodeStreams {

    @DisplayName("Set null, capped to zero")
    @Test
    void test1(PreparedStatement set, ColSnapshot<InputStream> streamtable) throws Exception {
      set.setUnicodeStream(1, null, 0);
      set.executeUpdate();
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set non-empty, capped to zero")
    @Test
    void test4(PreparedStatement set, @Random(codepoints = true) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setUnicodeStream(1, stream, 0);
      set.executeUpdate();
      stream = ByteStreams.limit(stream, 0);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by some value")
    @Test
    void test5(PreparedStatement set, @Random(codepoints = true) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setUnicodeStream(1, stream, 5);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 5);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by negative length")
    @Test
    void test6(PreparedStatement set, @Random(codepoints = true) InputStream stream, ColSnapshot<InputStream> streamtable) {
      assertThrows(SQLException.class, () -> set.setUnicodeStream(1, stream, -3));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException")
    @Test
    void test7(PreparedStatement set, @Random(codepoints = true) InputStream stream, ColSnapshot<InputStream> streamtable) {
      assertThrows(SQLException.class, () -> set.setUnicodeStream(1, new BrokenInputStream(stream, 3), 10));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<InputStream> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, limited before throws IOException")
    @Test
    void test7b(PreparedStatement set, @Random(codepoints = true) InputStream stream, ColSnapshot<String> streamtable) throws Exception {
      set.setUnicodeStream(1, new BrokenInputStream(stream, 3), 1);
      set.executeUpdate();
      stream.reset();
      stream = ByteStreams.limit(stream, 1);
      String txt = CharStreams.toString(new InputStreamReader(stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), equalTo(txt));
    }

  }

  @DisplayName("Character Streams")
  @Table(name = "streamtable", columns = {"str text"})
  @Prepare(name = "set", sql = "INSERT INTO streamtable (str) VALUES (?)")
  @Nested
  class CharacterStreams {

    @DisplayName("Set null")
    @Test
    void test1(PreparedStatement set, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, null);
      set.executeUpdate();
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set null, length specified")
    @Test
    void test1l(PreparedStatement set, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, null, 0);
      set.executeUpdate();
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(nullValue()));
    }

    @DisplayName("Set empty")
    @Test
    void test2(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, stream);
      set.executeUpdate();
      stream.reset();
      String txt = CharStreams.toString(stream);
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty")
    @Test
    void test3(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, stream);
      set.executeUpdate();
      stream.reset();
      String txt = CharStreams.toString(stream);
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped to zero")
    @Test
    void test4(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, stream, 0);
      set.executeUpdate();
      stream = CharStreams.limit(stream, 0);
      String txt = CharStreams.toString(stream);
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by some value")
    @Test
    void test5(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, stream, 5);
      set.executeUpdate();
      stream.reset();
      stream = CharStreams.limit(stream, 5);
      String txt = CharStreams.toString((stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data, hasItem(txt));
    }

    @DisplayName("Set non-empty, capped by negative length")
    @Test
    void test6(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setCharacterStream(1, stream, -3));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException")
    @Test
    void test7(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setCharacterStream(1, new BrokenReader(stream, 3)));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, throws IOException, length specified")
    @Test
    void test7b(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) {
      assertThrows(SQLException.class, () -> set.setCharacterStream(1, new BrokenReader(stream, 3), 10));
      assertThrows(SQLException.class, set::executeUpdate);
      Collection<String> data = streamtable.take();
      assertThat(data.size(), equalTo(0));
    }

    @DisplayName("Set non-empty, capped before throws IOException")
    @Test
    void test7c(PreparedStatement set, @Random Reader stream, ColSnapshot<String> streamtable) throws Exception {
      set.setCharacterStream(1, new BrokenReader(stream, 3), 1);
      set.executeUpdate();
      stream.reset();
      stream = CharStreams.limit(stream, 1);
      String txt = CharStreams.toString((stream));
      List<String> data = streamtable.take();
      assertThat(data.size(), equalTo(1));
      assertThat(data.get(0), equalTo(txt));
    }

  }

}
