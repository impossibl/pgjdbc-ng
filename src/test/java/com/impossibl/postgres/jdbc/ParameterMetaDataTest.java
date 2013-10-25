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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import junit.framework.TestCase;



public class ParameterMetaDataTest extends TestCase {

  private Connection _conn;

  public ParameterMetaDataTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "parametertest", "a int4, b float8, c text, d point, e timestamp with time zone");
  }

  protected void tearDown() throws SQLException {
    TestUtil.dropTable(_conn, "parametertest");
    TestUtil.closeDB(_conn);
  }

  public void testParameterMD() throws SQLException {

    PreparedStatement pstmt = _conn.prepareStatement("SELECT a FROM parametertest WHERE b = ? AND c = ? AND d >^ ? ");
    ParameterMetaData pmd = pstmt.getParameterMetaData();

    assertEquals(3, pmd.getParameterCount());
    assertEquals(Types.DOUBLE, pmd.getParameterType(1));
    assertEquals("float8", pmd.getParameterTypeName(1));
    assertEquals("java.lang.Double", pmd.getParameterClassName(1));
    assertEquals(Types.VARCHAR, pmd.getParameterType(2));
    assertEquals("text", pmd.getParameterTypeName(2));
    assertEquals("java.lang.String", pmd.getParameterClassName(2));
    assertEquals(Types.OTHER, pmd.getParameterType(3));
    assertEquals("point", pmd.getParameterTypeName(3));
    //assertEquals("org.postgresql.geometric.PGpoint", pmd.getParameterClassName(3));

    pstmt.close();
  }

  public void testFailsOnBadIndex() throws SQLException {

    PreparedStatement pstmt = _conn.prepareStatement("SELECT a FROM parametertest WHERE b = ? AND c = ?");
    ParameterMetaData pmd = pstmt.getParameterMetaData();
    try {
      pmd.getParameterType(0);
      fail("Can't get parameter for index < 1.");
    }
    catch(SQLException sqle) {
    }
    try {
      pmd.getParameterType(3);
      fail("Can't get parameter for index 3 with only two parameters.");
    }
    catch(SQLException sqle) {
    }
  }

  // Make sure we work when mashing two queries into a single statement.
//TODO: reconcile against mainstream driver
//  public void testMultiStatement() throws SQLException {
//
//    PreparedStatement pstmt = _conn.prepareStatement("SELECT a FROM parametertest WHERE b = ? AND c = ? ; SELECT b FROM parametertest WHERE a = ?");
//    ParameterMetaData pmd = pstmt.getParameterMetaData();
//
//    assertEquals(3, pmd.getParameterCount());
//    assertEquals(Types.DOUBLE, pmd.getParameterType(1));
//    assertEquals("float8", pmd.getParameterTypeName(1));
//    assertEquals(Types.VARCHAR, pmd.getParameterType(2));
//    assertEquals("text", pmd.getParameterTypeName(2));
//    assertEquals(Types.INTEGER, pmd.getParameterType(3));
//    assertEquals("int4", pmd.getParameterTypeName(3));
//
//    pstmt.close();
//
//  }

  // Here we test that we can legally change the resolved type
  // from text to varchar with the complicating factor that there
  // is also an unknown parameter.
  //
  public void testTypeChangeWithUnknown() throws SQLException {

    PreparedStatement pstmt = _conn.prepareStatement("SELECT a FROM parametertest WHERE c = ? AND e = ?");

    @SuppressWarnings("unused")
    ParameterMetaData pmd = pstmt.getParameterMetaData();

    pstmt.setString(1, "Hi");
    pstmt.setTimestamp(2, new Timestamp(0L));

    ResultSet rs = pstmt.executeQuery();
    rs.close();
  }

}
