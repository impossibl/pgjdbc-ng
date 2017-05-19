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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Michael Barker <mailto:mike@middlesoft.co.uk>
 *
 */
@RunWith(JUnit4.class)
public class BlobTest {

  private Connection conn;

  @Before
  public void before() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "blobtest", "ID INT PRIMARY KEY, DATA OID");
    TestUtil.createTable(conn, "testblob", "ID NAME, LO OID");
    conn.setAutoCommit(false);
  }

  @After
  public void after() throws SQLException {
    conn.setAutoCommit(true);
    TestUtil.dropTable(conn, "blobtest");
    TestUtil.dropTable(conn, "testblob");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testSetNull() throws Exception {

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testblob(lo) VALUES (?)");

    pstmt.setBlob(1, (Blob) null);
    pstmt.executeUpdate();

    pstmt.setNull(1, Types.BLOB);
    pstmt.executeUpdate();

    pstmt.setObject(1, null, Types.BLOB);
    pstmt.executeUpdate();

    pstmt.setClob(1, (Clob) null);
    pstmt.executeUpdate();

    pstmt.setNull(1, Types.CLOB);
    pstmt.executeUpdate();

    pstmt.setObject(1, null, Types.CLOB);
    pstmt.executeUpdate();

    pstmt.close();
  }

  @Test
  public void testSet() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("INSERT INTO testblob(id,lo) VALUES ('1', lo_creat(-1))");
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testblob(id, lo) VALUES(?,?)");

    Blob blob = rs.getBlob(1);
    pstmt.setString(1, "setObjectTypeBlob");
    pstmt.setObject(2, blob, Types.BLOB);
    assertEquals(1, pstmt.executeUpdate());

    blob = rs.getBlob(1);
    pstmt.setString(1, "setObjectBlob");
    pstmt.setObject(2, blob);
    assertEquals(1, pstmt.executeUpdate());

    blob = rs.getBlob(1);
    pstmt.setString(1, "setBlob");
    pstmt.setBlob(2, blob);
    assertEquals(1, pstmt.executeUpdate());

    Clob clob = rs.getClob(1);
    pstmt.setString(1, "setObjectTypeClob");
    pstmt.setObject(2, clob, Types.CLOB);
    assertEquals(1, pstmt.executeUpdate());

    clob = rs.getClob(1);
    pstmt.setString(1, "setObjectClob");
    pstmt.setObject(2, clob);
    assertEquals(1, pstmt.executeUpdate());

    clob = rs.getClob(1);
    pstmt.setString(1, "setClob");
    pstmt.setClob(2, clob);
    assertEquals(1, pstmt.executeUpdate());

    rs.close();
    stmt.close();
    pstmt.close();
  }

  /*
   * Tests uploading a blob to the database
   */
  @Test
  public void testUploadBlob() throws Exception {
    assertTrue(uploadFileBlob("pom.xml") > 0);

    assertTrue(compareBlobs());
  }

  /*
   * Tests uploading a clob to the database
   */
  @Test
  public void testUploadClob() throws Exception {
    assertTrue(uploadFileClob("pom.xml") > 0);

    assertTrue(compareClobs());
  }

  @Test
  public void testGetBytesOffsetBlob() throws Exception {
    assertTrue(uploadFileBlob("pom.xml") > 0);

    String eol = String.format("%n");

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Blob lob = rs.getBlob(1);
    int blobLength = 3 + eol.length();
    byte[] data = lob.getBytes(2, blobLength);
    assertEquals(data.length, blobLength);
    assertEquals(data[0], '!');
    assertEquals(data[1], '-');
    assertEquals(data[2], '-');

    for (int i = 0; i < eol.length(); i++) {
      assertEquals(data[3 + i], eol.charAt(i));
    }

    stmt.close();
    rs.close();
  }

  @Test
  public void testGetBytesOffsetClob() throws Exception {
    assertTrue(uploadFileClob("pom.xml") > 0);

    String eol = String.format("%n");

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Clob lob = rs.getClob(1);
    int blobLength = 3 + eol.length();
    String data = lob.getSubString(2, blobLength);
    assertEquals(data.length(), blobLength);
    assertEquals(data.charAt(0), '!');
    assertEquals(data.charAt(1), '-');
    assertEquals(data.charAt(2), '-');

    for (int i = 0; i < eol.length(); i++) {
      assertEquals(data.charAt(3 + i), eol.charAt(i));
    }

    stmt.close();
    rs.close();
  }

  @Test
  public void testMultipleStreamsBlob() throws Exception {
    assertTrue(uploadFileBlob("pom.xml") > 0);

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Blob lob = rs.getBlob(1);
    byte[] data = new byte[2];

    InputStream is = lob.getBinaryStream();
    assertEquals(data.length, is.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '!');
    is.close();

    is = lob.getBinaryStream();
    assertEquals(data.length, is.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '!');
    is.close();

    rs.close();
    stmt.close();
  }

  @Test
  public void testMultipleStreamsClob() throws Exception {
    assertTrue(uploadFileClob("pom.xml") > 0);

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Clob lob = rs.getClob(1);
    char[] data = new char[2];

    Reader r = lob.getCharacterStream();
    assertEquals(data.length, r.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '!');
    r.close();

    r = lob.getCharacterStream();
    assertEquals(data.length, r.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '!');
    r.close();

    rs.close();
    stmt.close();
  }

  @Test
  public void testParallelStreamsBlob() throws Exception {
    assertTrue(uploadFileBlob("pom.xml") > 0);

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Blob lob = rs.getBlob(1);
    InputStream is1 = lob.getBinaryStream();
    InputStream is2 = lob.getBinaryStream();

    while (true) {
      int i1 = is1.read();
      int i2 = is2.read();
      assertEquals(i1, i2);
      if (i1 == -1)
        break;
    }

    is1.close();
    is2.close();

    rs.close();
    stmt.close();
  }

  @Test
  public void testParallelStreamsClob() throws Exception {
    assertTrue(uploadFileClob("pom.xml") > 0);

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    assertTrue(rs.next());

    Clob lob = rs.getClob(1);
    Reader is1 = lob.getCharacterStream();
    Reader is2 = lob.getCharacterStream();

    while (true) {
      int i1 = is1.read();
      int i2 = is2.read();
      assertEquals(i1, i2);
      if (i1 == -1)
        break;
    }

    is1.close();
    is2.close();

    rs.close();
    stmt.close();
  }

  private long uploadFileBlob(String file) throws Exception {

    FileInputStream fis = new FileInputStream(file);

    int oid = LargeObject.creat((PGConnectionImpl) conn, LargeObject.INV_WRITE);
    LargeObject lo = LargeObject.open((PGConnectionImpl) conn, oid);

    OutputStream os = new BlobOutputStream(null, lo.dup());
    int s = fis.read();
    while (s > -1) {
      os.write(s);
      s = fis.read();
    }
    os.close();

    lo.close();
    fis.close();

    // Insert into the table
    Statement st = conn.createStatement();
    st.executeUpdate(TestUtil.insertSQL("testblob", "id,lo", "'" + file + "'," + oid));
    conn.commit();
    st.close();

    return oid;
  }

  private long uploadFileClob(String file) throws Exception {

    FileReader fr = new FileReader(file);

    int oid = LargeObject.creat((PGConnectionImpl) conn, LargeObject.INV_WRITE);
    LargeObject lo = LargeObject.open((PGConnectionImpl) conn, oid);

    ClobWriter cw = new ClobWriter(null, lo.dup());
    int ch = fr.read();
    while (ch > -1) {
      cw.write(ch);
      ch = fr.read();
    }
    cw.close();

    lo.close();
    fr.close();

    // Insert into the table
    Statement st = conn.createStatement();
    st.executeUpdate(TestUtil.insertSQL("testblob", "id,lo", "'" + file + "'," + oid));
    conn.commit();
    st.close();

    return oid;
  }

  /*
   * Helper - compares the blobs in a table with a local file. This uses the
   * jdbc java.sql.Blob api
   */
  private boolean compareBlobs() throws Exception {
    boolean result = true;

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery(TestUtil.selectSQL("testblob", "id,lo"));
    assertNotNull(rs);

    while (rs.next()) {
      String file = rs.getString(1);
      Blob blob = rs.getBlob(2);

      FileInputStream fis = new FileInputStream(file);
      InputStream bis = blob.getBinaryStream();

      int f = fis.read();
      int b = bis.read();
      int c = 0;
      while (f >= 0 && b >= 0 & result) {
        result = (f == b);
        f = fis.read();
        b = bis.read();
        c++;
      }
      result = result && f == -1 && b == -1;

      if (!result)
        assertTrue("Blob compare failed at " + c + " of " + blob.length(), false);

      bis.close();
      fis.close();
    }
    rs.close();
    st.close();

    return result;
  }

  /*
   * Helper - compares the clobs in a table with a local file.
   */
  private boolean compareClobs() throws Exception {
    boolean result = true;

    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery(TestUtil.selectSQL("testblob", "id,lo"));
    assertNotNull(rs);

    while (rs.next()) {
      String file = rs.getString(1);
      Clob clob = rs.getClob(2);

      FileReader fr = new FileReader(file);
      Reader cr = clob.getCharacterStream();

      int f = fr.read();
      int b = cr.read();
      int c = 0;
      while (f >= 0 && b >= 0 & result) {
        result = (f == b);
        f = fr.read();
        b = cr.read();
        c++;
      }
      result = result && f == -1 && b == -1;

      if (!result)
        assertTrue("Clob compare failed at " + c + " of " + clob.length(), false);

      cr.close();
      fr.close();
    }
    rs.close();
    st.close();

    return result;
  }

  /**
   * Test the writing and reading of a single byte.
   *
   * @throws SQLException
   */
  @Test
  public void test1Byte() throws SQLException {
    byte[] data = {(byte) 'a'};
    readWriteBlob(data);
  }

  /**
   * Test the writing and reading of a single char.
   *
   * @throws SQLException
   */
  @Test
  public void test1Char() throws SQLException {
    String data = "a";
    readWriteClob(data);
  }

  /**
   * Test the writing and reading of a few bytes.
   *
   * @throws SQLException
   */
  @Test
  public void testManyBytes() throws SQLException {
    byte[] data = "aaaaaaaaaa".getBytes();
    readWriteBlob(data);
  }

  /**
   * Test the writing and reading of a few chars.
   *
   * @throws SQLException
   */
  @Test
  public void testManyChars() throws SQLException {
    String data = "aaaaaaaaaa";
    readWriteClob(data);
  }

  /**
   * Test writing a single byte with an offset.
   *
   * @throws SQLException
   */
  @Test
  public void test1ByteOffset() throws SQLException {
    byte[] data = {(byte) 'a'};
    readWriteBlob(10, data);
  }

  /**
   * Test writing a single char with an offset.
   *
   * @throws SQLException
   */
  @Test
  public void test1CharOffset() throws SQLException {
    String data = "a";
    readWriteClob(10, data);
  }

  /**
   * Test the writing and reading of a few bytes with an offset.
   *
   * @throws SQLException
   */
  @Test
  public void testManyBytesOffset() throws SQLException {
    byte[] data = "aaaaaaaaaa".getBytes();
    readWriteBlob(10, data);
  }

  /**
   * Test the writing and reading of a few chars with an offset.
   *
   * @throws SQLException
   */
  @Test
  public void testManyCharsOffset() throws SQLException {
    String data = "aaaaaaaaaa";
    readWriteClob(10, data);
  }

  /**
   * Tests all of the byte values from 0 - 255.
   *
   * @throws SQLException
   */
  @Test
  public void testAllBytes() throws SQLException {
    byte[] data = new byte[256];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    readWriteBlob(data);
  }

  /**
   * Tests random values across entire code point range
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testRangeChars() throws SQLException, IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < 256; i++) {
      dos.writeInt(i * 16 * 1024 * 1024);
    }
    readWriteClob(new String(bos.toByteArray(), PGClob.CHARSET));
  }

  @Test
  public void testTruncateBlob() throws SQLException {

    byte[] data = new byte[100];
    for (byte i = 0; i < data.length; i++) {
      data[i] = i;
    }
    readWriteBlob(data);

    PreparedStatement ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Blob blob = rs.getBlob("DATA");

    assertEquals(100, blob.length());

    blob.truncate(50);
    assertEquals(50, blob.length());

    blob.truncate(150);
    assertEquals(150, blob.length());

    data = blob.getBytes(1, 200);
    assertEquals(150, data.length);
    for (byte i = 0; i < 50; i++) {
      assertEquals(i, data[i]);
    }

    for (int i = 50; i < 150; i++) {
      assertEquals(0, data[i]);
    }

    rs.close();
    ps.close();
  }

  @Test
  public void testTruncateClob() throws SQLException {

    char[] chars = new char[100];
    for (char i = 0; i < chars.length; i++) {
      chars[i] = i;
    }
    String data = new String(chars);

    readWriteClob(data);

    PreparedStatement ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Clob clob = rs.getClob("DATA");

    assertEquals(100, clob.length());

    clob.truncate(50);
    assertEquals(50, clob.length());

    clob.truncate(150);
    assertEquals(150, clob.length());

    data = clob.getSubString(1, 200);
    assertEquals(150, data.length());
    for (char i = 0; i < 50; i++) {
      assertEquals(i, data.charAt(i));
    }

    for (int i = 50; i < 150; i++) {
      assertEquals(0, data.charAt(i));
    }

    rs.close();
    ps.close();
  }

  /**
   *
   * @param data
   * @throws SQLException
   */
  private void readWriteBlob(byte[] data) throws SQLException {
    readWriteBlob(1, data);
  }

  /**
   *
   * @param data
   * @throws SQLException
   */
  private void readWriteClob(String data) throws SQLException {
    readWriteClob(1, data);
  }

  /**
   *
   * @param offset
   * @param data
   * @throws SQLException
   */
  private void readWriteBlob(int offset, byte[] data) throws SQLException {

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Blob b = rs.getBlob("DATA");
    b.setBytes(offset, data);

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    b = rs.getBlob("DATA");
    byte[] rspData = b.getBytes(offset, data.length);
    assertTrue("Request should be the same as the response", Arrays.equals(data, rspData));

    rs.close();
    ps.close();
  }

  /**
   *
   * @param offset
   * @param data
   * @throws SQLException
   */
  private void readWriteClob(int offset, String data) throws SQLException {

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Clob b = rs.getClob("DATA");
    b.setString(offset, data);

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    b = rs.getClob("DATA");
    String rspData = b.getSubString(offset, data.length());
    assertEquals("Request should be the same as the response", data, rspData);

    rs.close();
    ps.close();
  }

  /**
   * Test the writing and reading of a single byte.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void test1ByteStream() throws SQLException, IOException {
    byte[] data = {(byte) 'a'};
    readWriteBlobStream(data);
  }

  /**
   * Test the writing and reading of a single char.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void test1CharStream() throws SQLException, IOException {
    String data = "a";
    readWriteClobStream(data);
  }

  /**
   * Test the writing and reading of a few bytes.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testManyBytesStream() throws SQLException, IOException {
    byte[] data = "aaaaaaaaaa".getBytes();
    readWriteBlobStream(data);
  }

  /**
   * Test the writing and reading of a few chars.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testManyCharsStream() throws SQLException, IOException {
    String data = "aaaaaaaaaa";
    readWriteClobStream(data);
  }

  /**
   * Test writing a single byte with an offset.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void test1ByteOffsetStream() throws SQLException, IOException {
    byte[] data = {(byte) 'a'};
    readWriteBlobStream(10, data);
  }

  /**
   * Test writing a single char with an offset.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void test1CharOffsetStream() throws SQLException, IOException {
    String data = "a";
    readWriteClobStream(10, data);
  }

  /**
   * Test the writing and reading of a few bytes with an offset.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testManyBytesOffsetStream() throws SQLException, IOException {
    byte[] data = "aaaaaaaaaa".getBytes();
    readWriteBlobStream(10, data);
  }

  /**
   * Test the writing and reading of a few chars with an offset.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testManyCharsOffsetStream() throws SQLException, IOException {
    String data = "aaaaaaaaaa";
    readWriteClobStream(10, data);
  }

  /**
   * Tests all of the byte values from 0 - 255.
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testAllBytesStream() throws SQLException, IOException {
    byte[] data = new byte[256];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    readWriteBlobStream(data);
  }

  /**
   * Tests random values across entire code point range
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  public void testRangeCharsStream() throws SQLException, IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < 256; i++) {
      dos.writeInt(i * 16 * 1024 * 1024);
    }
    readWriteClobStream(new String(bos.toByteArray(), PGClob.CHARSET));
  }

  private void readWriteBlobStream(byte[] data) throws SQLException, IOException {
    readWriteBlobStream(1, data);
  }

  private void readWriteClobStream(String data) throws SQLException, IOException {
    readWriteClobStream(1, data);
  }

  /**
   * Reads then writes data to the blob via a stream.
   *
   * @param offset
   * @param data
   * @throws SQLException
   * @throws IOException
   */
  private void readWriteBlobStream(int offset, byte[] data) throws SQLException, IOException {

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Blob b = rs.getBlob("DATA");
    OutputStream out = b.setBinaryStream(offset);
    out.write(data);
    out.flush();
    out.close();

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    b = rs.getBlob("DATA");
    InputStream in = b.getBinaryStream();
    byte[] rspData = new byte[data.length];
    in.skip(offset - 1);
    in.read(rspData);
    in.close();

    assertTrue("Request should be the same as the response", Arrays.equals(data, rspData));

    rs.close();
    ps.close();
  }

  /**
   * Reads then writes data to the clob via a stream.
   *
   * @param offset
   * @param data
   * @throws SQLException
   * @throws IOException
   */
  private void readWriteClobStream(int offset, String data) throws SQLException, IOException {

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Clob c = rs.getClob("DATA");
    Writer out = c.setCharacterStream(offset);
    out.write(data);
    out.flush();
    out.close();

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    c = rs.getClob("DATA");
    Reader in = c.getCharacterStream();
    char[] rspData = new char[data.length()];
    in.skip(offset - 1);
    in.read(rspData);
    in.close();

    assertEquals("Request should be the same as the response", data, new String(rspData));

    rs.close();
    ps.close();
  }

  @Test
  public void testPatternBlob() throws SQLException {
    byte[] data = "abcdefghijklmnopqrstuvwxyx0123456789".getBytes();
    byte[] pattern = "def".getBytes();

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Blob b = rs.getBlob("DATA");
    b.setBytes(1, data);

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    b = rs.getBlob("DATA");
    long position = b.position(pattern, 1);
    byte[] rspData = b.getBytes(position, pattern.length);
    assertTrue("Request should be the same as the response", Arrays.equals(pattern, rspData));

    rs.close();
    ps.close();

  }

  @Test
  public void testPatternClob() throws SQLException {
    String data = "abcdefghijklmnopqrstuvwxyx0123456789";
    String pattern = "def";

    PreparedStatement ps = conn.prepareStatement("INSERT INTO blobtest VALUES (1, lo_creat(-1))");
    ps.executeUpdate();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    ResultSet rs = ps.executeQuery();

    assertTrue(rs.next());
    Clob c = rs.getClob("DATA");
    c.setString(1, data);

    rs.close();
    ps.close();

    ps = conn.prepareStatement("SELECT ID, DATA FROM blobtest WHERE ID = 1");
    rs = ps.executeQuery();

    assertTrue(rs.next());
    c = rs.getClob("DATA");
    long position = c.position(pattern, 1);
    String rspData = c.getSubString(position, pattern.length());
    assertEquals("Request should be the same as the response", pattern, rspData);

    rs.close();
    ps.close();

  }

  @Test
  public void testFreeBlob() throws SQLException {
    Statement stmt = conn.createStatement();

    stmt.execute("INSERT INTO blobtest VALUES (1, lo_creat(-1))");

    ResultSet rs = stmt.executeQuery("SELECT data FROM blobtest");
    assertTrue(rs.next());

    Blob blob = rs.getBlob(1);
    blob.free();
    try {
      blob.length();
      fail("Should have thrown an Exception because it was freed.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    rs.close();
    stmt.close();
  }

  @Test
  public void testFreeClob() throws SQLException {
    Statement stmt = conn.createStatement();

    stmt.execute("INSERT INTO blobtest VALUES (1, lo_creat(-1))");

    ResultSet rs = stmt.executeQuery("SELECT data FROM blobtest");
    assertTrue(rs.next());

    Clob clob = rs.getClob(1);
    clob.free();
    try {
      clob.length();
      fail("Should have thrown an Exception because it was freed.");
    }
    catch (SQLException sqle) {
      // Ok
    }

    rs.close();
    stmt.close();
  }

  @Test
  public void testEOFBlob() throws SQLException, IOException {
    Statement stmt = conn.createStatement();

    stmt.execute("INSERT INTO blobtest VALUES (1, lo_creat(-1))");

    ResultSet rs = stmt.executeQuery("SELECT data FROM blobtest");
    assertTrue(rs.next());

    Blob blob = rs.getBlob(1);

    InputStream in = blob.getBinaryStream();

    assertEquals(-1, in.read());
    assertEquals(-1, in.read(new byte[4], 0, 4));

    in.close();
    blob.free();
    rs.close();
    stmt.close();
  }

  @Test
  public void testEOFClob() throws SQLException, IOException {
    Statement stmt = conn.createStatement();

    stmt.execute("INSERT INTO blobtest VALUES (1, lo_creat(-1))");

    ResultSet rs = stmt.executeQuery("SELECT data FROM blobtest");
    assertTrue(rs.next());

    Clob clob = rs.getClob(1);

    Reader in = clob.getCharacterStream();

    assertEquals(-1, in.read());
    assertEquals(-1, in.read(new char[4], 0, 4));

    in.close();
    clob.free();
    rs.close();
    stmt.close();
  }

  @Test
  public void testWrapperBlob() throws SQLException {
    conn.setAutoCommit(false);

    PreparedStatement stmt = conn.prepareStatement("INSERT INTO blobtest VALUES (1, ?)");

    final Blob blob = conn.createBlob();

    Blob wrapper = new Blob() {

      @Override
      public long length() throws SQLException {
        return blob.length();
      }

      @Override
      public byte[] getBytes(long pos, int length) throws SQLException {
        return blob.getBytes(pos, length);
      }

      @Override
      public InputStream getBinaryStream() throws SQLException {
        return blob.getBinaryStream();
      }

      @Override
      public long position(byte[] pattern, long start) throws SQLException {
        return blob.position(pattern, start);
      }

      @Override
      public long position(Blob pattern, long start) throws SQLException {
        return blob.position(pattern, start);
      }

      @Override
      public int setBytes(long pos, byte[] bytes) throws SQLException {
        return blob.setBytes(pos, bytes);
      }

      @Override
      public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return blob.setBytes(pos, bytes, offset, len);
      }

      @Override
      public OutputStream setBinaryStream(long pos) throws SQLException {
        return blob.setBinaryStream(pos);
      }

      @Override
      public void truncate(long len) throws SQLException {
        blob.truncate(len);
      }

      @Override
      public void free() throws SQLException {
        blob.free();
      }

      @Override
      public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return blob.getBinaryStream(pos, length);
      }

    };

    stmt.setBlob(1, wrapper);

    stmt.execute();

    stmt.close();

    conn.commit();
  }

  @Test
  public void testWrapperClob() throws SQLException {
    conn.setAutoCommit(false);

    PreparedStatement stmt = conn.prepareStatement("INSERT INTO blobtest VALUES (1, ?)");

    final Clob clob = conn.createClob();

    Clob wrapper = new Clob() {

      @Override
      public long length() throws SQLException {
        return clob.length();
      }

      @Override
      public String getSubString(long pos, int length) throws SQLException {
        return clob.getSubString(pos, length);
      }

      @Override
      public Reader getCharacterStream() throws SQLException {
        return clob.getCharacterStream();
      }

      @Override
      public InputStream getAsciiStream() throws SQLException {
        return clob.getAsciiStream();
      }

      @Override
      public long position(String searchstr, long start) throws SQLException {
        return clob.position(searchstr, start);
      }

      @Override
      public long position(Clob searchstr, long start) throws SQLException {
        return clob.position(searchstr, start);
      }

      @Override
      public int setString(long pos, String str) throws SQLException {
        return clob.setString(pos, str);
      }

      @Override
      public int setString(long pos, String str, int offset, int len) throws SQLException {
        return clob.setString(pos, str, offset, len);
      }

      @Override
      public OutputStream setAsciiStream(long pos) throws SQLException {
        return clob.setAsciiStream(pos);
      }

      @Override
      public Writer setCharacterStream(long pos) throws SQLException {
        return clob.setCharacterStream(pos);
      }

      @Override
      public void truncate(long len) throws SQLException {
        clob.truncate(len);
      }

      @Override
      public void free() throws SQLException {
        clob.free();
      }

      @Override
      public Reader getCharacterStream(long pos, long length) throws SQLException {
        return clob.getCharacterStream(pos, length);
      }

    };

    stmt.setClob(1, wrapper);

    stmt.execute();

    stmt.close();

    conn.commit();
  }

  @Test
  public void testBlobClose() throws Exception {
    final Blob blob = conn.createBlob();
    try {
      OutputStream blobOutputStream = blob.setBinaryStream(1L);
      try {
        java.io.BufferedOutputStream bufferedOutputStream =
            new java.io.BufferedOutputStream(blobOutputStream);
        try {
          bufferedOutputStream.write(100);
        }
        finally {
          bufferedOutputStream.close();
        }
      }
      finally {
        blobOutputStream.close();
      }
    }
    finally {
      blob.free();
    }
  }

  @Test
  public void testBlobCloseWithResources() throws Exception {
    final Blob blob = conn.createBlob();
    try {
      OutputStream blobOutputStream = blob.setBinaryStream(1L);
      try (java.io.BufferedOutputStream bufferedOutputStream =
               new java.io.BufferedOutputStream(blobOutputStream)) {
        bufferedOutputStream.write(100);
      }
      finally {
        blobOutputStream.close();
      }
    }
    finally {
      blob.free();
    }
  }

}
