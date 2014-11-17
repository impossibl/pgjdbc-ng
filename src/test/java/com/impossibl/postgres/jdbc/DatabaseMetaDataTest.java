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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.PseudoColumnUsage;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/*
 * TestCase to test the internal functionality of org.postgresql.jdbc2.DatabaseMetaData
 *
 */
@RunWith(JUnit4.class)
public class DatabaseMetaDataTest {

  private Connection con;

  @Before
  public void before() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTable(con, "metadatatest", "id int4, name text, updated timestamptz, colour text, quest text");
    TestUtil.dropSequence(con, "sercoltest_b_seq");
    TestUtil.dropSequence(con, "sercoltest_c_seq");
    TestUtil.createTable(con, "sercoltest", "a int, b serial, c bigserial");
    TestUtil.createTable(con, "\"a\\\"", "a int4");
    TestUtil.createTable(con, "\"a'\"", "a int4");
    TestUtil.createTable(con, "arraytable", "a numeric(5,2)[], b varchar(100)[]");

    Statement stmt = con.createStatement();
    // we add the following comments to ensure the joins to the comments
    // are done correctly. This ensures we correctly test that case.
    stmt.execute("comment on table metadatatest is 'this is a table comment'");
    stmt.execute("comment on column metadatatest.id is 'this is a column comment'");

    stmt.execute("CREATE OR REPLACE FUNCTION f1(int, varchar) RETURNS int AS 'SELECT 1;' LANGUAGE SQL");
    stmt.execute("CREATE OR REPLACE FUNCTION f2(a int, b varchar) RETURNS int AS 'SELECT 1;' LANGUAGE SQL");
    stmt.execute("CREATE OR REPLACE FUNCTION f3(IN a int, INOUT b varchar, OUT c timestamptz) AS $f$ BEGIN b := 'a'; c := now(); return; END; $f$ LANGUAGE plpgsql");
    stmt.execute("CREATE OR REPLACE FUNCTION f4(int) RETURNS metadatatest AS 'SELECT 1, ''a''::text, now(), ''c''::text, ''q''::text' LANGUAGE SQL");
    stmt.execute("DROP DOMAIN IF EXISTS nndom CASCADE");
    stmt.execute("DROP TABLE IF EXISTS domaintable CASCADE");
    stmt.execute("CREATE DOMAIN nndom AS int not null");
    stmt.execute("CREATE TABLE domaintable (id nndom)");

    TestUtil.createType(con, "attr_test", "val1 text, val2 numeric(8,3), val3 nndom");
    stmt.execute("comment on column attr_test.val1 is 'this is a attribute comment'");

    stmt.close();
  }

  @After
  public void after() throws Exception {
    // Drop function first because it depends on the
    // metadatatest table's type
    Statement stmt = con.createStatement();
    stmt.execute("DROP FUNCTION f4(int)");

    TestUtil.dropTable(con, "metadatatest");
    TestUtil.dropTable(con, "sercoltest");
    TestUtil.dropSequence(con, "sercoltest_b_seq");
    TestUtil.dropSequence(con, "sercoltest_c_seq");
    TestUtil.dropTable(con, "\"a\\\"");
    TestUtil.dropTable(con, "\"a'\"");
    TestUtil.dropTable(con, "arraytable");
    TestUtil.dropType(con, "attr_test");

    stmt.execute("DROP FUNCTION f1(int, varchar)");
    stmt.execute("DROP FUNCTION f2(int, varchar)");
    stmt.execute("DROP FUNCTION f3(int, varchar)");
    stmt.execute("DROP TABLE domaintable");
    stmt.execute("DROP DOMAIN nndom");

    stmt.close();

    TestUtil.closeDB(con);
  }

  @Test
  public void testTables() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    ResultSet rs = dbmd.getTables(null, null, "metadatates%", new String[] {"TABLE"});
    assertTrue(rs.next());
    String tableName = rs.getString("TABLE_NAME");
    assertEquals("metadatatest", tableName);
    String tableType = rs.getString("TABLE_TYPE");
    assertEquals("TABLE", tableType);
    // There should only be one row returned
    assertTrue("getTables() returned too many rows", rs.next() == false);
    rs.close();

    rs = dbmd.getColumns(null, null, "meta%", "%");
    assertTrue(rs.next());
    assertEquals("metadatatest", rs.getString("TABLE_NAME"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));

    assertTrue(rs.next());
    assertEquals("metadatatest", rs.getString("TABLE_NAME"));
    assertEquals("name", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.VARCHAR, rs.getInt("DATA_TYPE"));

    assertTrue(rs.next());
    assertEquals("metadatatest", rs.getString("TABLE_NAME"));
    assertEquals("updated", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
    rs.close();
  }

  @Test
  public void testCrossReference() throws Exception {
    Connection con1 = TestUtil.openDB();

    TestUtil.createTable(con1, "vv", "a int not null, b int not null, primary key ( a, b )");

    TestUtil.createTable(con1, "ww", "m int not null, n int not null, primary key ( m, n ), foreign key ( m, n ) references vv ( a, b )");

    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    ResultSet rs = dbmd.getCrossReference(null, "", "vv", null, "", "ww");

    for (int j = 1; rs.next(); j++) {

      String pkTableName = rs.getString("PKTABLE_NAME");
      assertEquals("vv", pkTableName);

      String pkColumnName = rs.getString("PKCOLUMN_NAME");
      assertTrue(pkColumnName.equals("a") || pkColumnName.equals("b"));

      String fkTableName = rs.getString("FKTABLE_NAME");
      assertEquals("ww", fkTableName);

      String fkColumnName = rs.getString("FKCOLUMN_NAME");
      assertTrue(fkColumnName.equals("m") || fkColumnName.equals("n"));

      String fkName = rs.getString("FK_NAME");
      assertEquals("ww_m_fkey", fkName);

      String pkName = rs.getString("PK_NAME");
      assertEquals("vv_pkey", pkName);

      int keySeq = rs.getInt("KEY_SEQ");
      assertEquals(j, keySeq);
    }

    rs.close();

    TestUtil.dropTable(con1, "vv");
    TestUtil.dropTable(con1, "ww");
    TestUtil.closeDB(con1);
  }

  @Test
  public void testForeignKeyActions() throws Exception {
    Connection conn = TestUtil.openDB();
    TestUtil.createTable(conn, "pkt", "id int primary key");
    TestUtil.createTable(conn, "fkt1", "id int references pkt on update restrict on delete cascade");
    TestUtil.createTable(conn, "fkt2", "id int references pkt on update set null on delete set default");
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = dbmd.getImportedKeys(null, "", "fkt1");
    assertTrue(rs.next());
    assertTrue(rs.getInt("UPDATE_RULE") == DatabaseMetaData.importedKeyRestrict);
    assertTrue(rs.getInt("DELETE_RULE") == DatabaseMetaData.importedKeyCascade);
    rs.close();

    rs = dbmd.getImportedKeys(null, "", "fkt2");
    assertTrue(rs.next());
    assertTrue(rs.getInt("UPDATE_RULE") == DatabaseMetaData.importedKeySetNull);
    assertTrue(rs.getInt("DELETE_RULE") == DatabaseMetaData.importedKeySetDefault);
    rs.close();

    TestUtil.dropTable(conn, "fkt2");
    TestUtil.dropTable(conn, "fkt1");
    TestUtil.dropTable(conn, "pkt");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testForeignKeysToUniqueIndexes() throws Exception {
    Connection con1 = TestUtil.openDB();
    TestUtil.createTable(con1, "pkt", "a int not null, b int not null, CONSTRAINT pkt_pk_a PRIMARY KEY (a), CONSTRAINT pkt_un_b UNIQUE (b)");
    TestUtil.createTable(con1, "fkt", "c int, d int, CONSTRAINT fkt_fk_c FOREIGN KEY (c) REFERENCES pkt(b)");

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getImportedKeys("", "", "fkt");
    int j = 0;
    for (; rs.next(); j++) {
      assertTrue("pkt".equals(rs.getString("PKTABLE_NAME")));
      assertTrue("fkt".equals(rs.getString("FKTABLE_NAME")));
      assertTrue("pkt_un_b".equals(rs.getString("PK_NAME")));
      assertTrue("b".equals(rs.getString("PKCOLUMN_NAME")));
    }
    assertTrue(j == 1);

    rs.close();

    TestUtil.dropTable(con1, "fkt");
    TestUtil.dropTable(con1, "pkt");
    con1.close();
  }

  @Test
  public void testMultiColumnForeignKeys() throws Exception {
    Connection con1 = TestUtil.openDB();
    TestUtil.createTable(con1, "pkt", "a int not null, b int not null, CONSTRAINT pkt_pk PRIMARY KEY (a,b)");
    TestUtil.createTable(con1, "fkt", "c int, d int, CONSTRAINT fkt_fk_pkt FOREIGN KEY (c,d) REFERENCES pkt(b,a)");

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getImportedKeys("", "", "fkt");
    int j = 0;
    for (; rs.next(); j++) {
      assertTrue("pkt".equals(rs.getString("PKTABLE_NAME")));
      assertTrue("fkt".equals(rs.getString("FKTABLE_NAME")));
      assertTrue(j + 1 == rs.getInt("KEY_SEQ"));
      if (j == 0) {
        assertTrue("b".equals(rs.getString("PKCOLUMN_NAME")));
        assertTrue("c".equals(rs.getString("FKCOLUMN_NAME")));
      }
      else {
        assertTrue("a".equals(rs.getString("PKCOLUMN_NAME")));
        assertTrue("d".equals(rs.getString("FKCOLUMN_NAME")));
      }
    }
    assertTrue(j == 2);

    rs.close();

    TestUtil.dropTable(con1, "fkt");
    TestUtil.dropTable(con1, "pkt");
    con1.close();
  }

  @Test
  public void testForeignKeys() throws Exception {
    Connection con1 = TestUtil.openDB();
    TestUtil.createTable(con1, "people", "id int4 primary key, name text");
    TestUtil.createTable(con1, "policy", "id int4 primary key, name text");

    TestUtil.createTable(con1, "users", "id int4 primary key, people_id int4, policy_id int4," + "CONSTRAINT people FOREIGN KEY (people_id) references people(id),"
        + "constraint policy FOREIGN KEY (policy_id) references policy(id)");

    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    ResultSet rs = dbmd.getImportedKeys(null, "", "users");
    int j = 0;
    for (; rs.next(); j++) {

      String pkTableName = rs.getString("PKTABLE_NAME");
      assertTrue(pkTableName.equals("people") || pkTableName.equals("policy"));

      String pkColumnName = rs.getString("PKCOLUMN_NAME");
      assertEquals("id", pkColumnName);

      String fkTableName = rs.getString("FKTABLE_NAME");
      assertEquals("users", fkTableName);

      String fkColumnName = rs.getString("FKCOLUMN_NAME");
      assertTrue(fkColumnName.equals("people_id") || fkColumnName.equals("policy_id"));

      String fkName = rs.getString("FK_NAME");
      assertTrue(fkName.startsWith("people") || fkName.startsWith("policy"));

      String pkName = rs.getString("PK_NAME");
      assertTrue(pkName.equals("people_pkey") || pkName.equals("policy_pkey"));

    }

    assertTrue(j == 2);

    rs.close();

    rs = dbmd.getExportedKeys(null, "", "people");

    // this is hacky, but it will serve the purpose
    assertTrue(rs.next());

    assertEquals("people", rs.getString("PKTABLE_NAME"));
    assertEquals("id", rs.getString("PKCOLUMN_NAME"));

    assertEquals("users", rs.getString("FKTABLE_NAME"));
    assertEquals("people_id", rs.getString("FKCOLUMN_NAME"));

    assertTrue(rs.getString("FK_NAME").startsWith("people"));

    rs.close();

    TestUtil.dropTable(con1, "users");
    TestUtil.dropTable(con1, "people");
    TestUtil.dropTable(con1, "policy");
    TestUtil.closeDB(con1);
  }

  @Test
  public void testColumns() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getColumns(null, null, "pg_class", null);
    rs.close();
  }

  @Test
  public void testDroppedColumns() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("ALTER TABLE metadatatest DROP name");
    stmt.execute("ALTER TABLE metadatatest DROP colour");
    stmt.close();

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "metadatatest", null);

    assertTrue(rs.next());
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));

    assertTrue(rs.next());
    assertEquals("updated", rs.getString("COLUMN_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));

    assertTrue(rs.next());
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals(3, rs.getInt("ORDINAL_POSITION"));

    rs.close();

    rs = dbmd.getColumns(null, null, "metadatatest", "quest");
    assertTrue(rs.next());
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals(3, rs.getInt("ORDINAL_POSITION"));
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testPseudoColumns() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getPseudoColumns(null, null, "pg_class", "ctid");
    assertTrue(rs.next());
    assertNull(rs.getString("TABLE_CAT"));
    assertEquals("pg_catalog", rs.getString("TABLE_SCHEM"));
    assertEquals("pg_class", rs.getString("TABLE_NAME"));
    assertEquals("ctid", rs.getString("COLUMN_NAME"));
    assertEquals(Types.ROWID, rs.getInt("DATA_TYPE"));
    assertEquals(6, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertEquals(PseudoColumnUsage.NO_USAGE_RESTRICTIONS.name(), rs.getString("COLUMN_USAGE"));
    assertNull(rs.getString("REMARKS"));
    assertEquals(6, rs.getInt("CHAR_OCTET_LENGTH"));
    assertEquals("NO", rs.getString("IS_NULLABLE"));
    rs.close();
  }

  @Test
  public void testSerialColumns() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "sercoltest", null);
    int rownum = 0;
    while (rs.next()) {
      assertEquals("sercoltest", rs.getString("TABLE_NAME"));
      assertEquals(rownum + 1, rs.getInt("ORDINAL_POSITION"));
      if (rownum == 0) {
        assertEquals("int4", rs.getString("TYPE_NAME"));
      }
      else if (rownum == 1) {
        assertEquals("serial", rs.getString("TYPE_NAME"));
      }
      else if (rownum == 2) {
        assertEquals("bigserial", rs.getString("TYPE_NAME"));
      }
      rownum++;
    }
    assertEquals(3, rownum);
    rs.close();
  }

  @Test
  public void testColumnPrivileges() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getColumnPrivileges(null, null, "pg_statistic", null);
    rs.close();
  }

  @Test
  public void testTablePrivileges() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getTablePrivileges(null, null, "metadatatest");
    boolean l_foundSelect = false;
    while (rs.next()) {
      if (rs.getString("GRANTEE").equals(TestUtil.getUser()) && rs.getString("PRIVILEGE").equals("SELECT"))
        l_foundSelect = true;
    }
    rs.close();
    // Test that the table owner has select priv
    assertTrue("Couldn't find SELECT priv on table metadatatest for " + TestUtil.getUser(), l_foundSelect);
  }

  @Test
  public void testNoTablePrivileges() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("REVOKE ALL ON metadatatest FROM PUBLIC");
    stmt.execute("REVOKE ALL ON metadatatest FROM " + TestUtil.getUser());
    stmt.close();

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getTablePrivileges(null, null, "metadatatest");
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testPrimaryKeys() throws Exception {

    Connection conn = TestUtil.openDB();
    TestUtil.createTable(conn, "pkt2", "id int, id2 int, CONSTRAINT pkt2_pkey PRIMARY KEY (id, id2)");
    try {

      DatabaseMetaData dbmd = con.getMetaData();
      assertNotNull(dbmd);
      ResultSet rs = dbmd.getPrimaryKeys(null, null, "pkt2");

      while (rs.next()) {

        assertEquals("public", rs.getString("TABLE_SCHEM"));
        assertEquals("pkt2", rs.getString("TABLE_NAME"));

        String colName = rs.getString("COLUMN_NAME");
        assertTrue(colName.equals("id") || colName.equals("id2"));

        assertEquals("pkt2_pkey", rs.getString("PK_NAME"));
      }

      rs.close();

    }
    finally {
      TestUtil.dropTable(conn, "pkt2");
      TestUtil.closeDB(conn);
    }

  }

  @Test
  public void testIndexInfo() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("create index idx_id on metadatatest (id)");
    stmt.execute("create index idx_func_single on metadatatest (upper(colour))");
    stmt.execute("create unique index idx_un_id on metadatatest(id)");
    stmt.execute("create index idx_func_multi on metadatatest (upper(colour), upper(quest))");
    stmt.execute("create index idx_func_mixed on metadatatest (colour, upper(quest))");
    stmt.close();

    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next());
    assertEquals("idx_un_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertTrue(!rs.getBoolean("NON_UNIQUE"));

    assertTrue(rs.next());
    assertEquals("idx_func_mixed", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("colour", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("idx_func_mixed", rs.getString("INDEX_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(quest)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("idx_func_multi", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(colour)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("idx_func_multi", rs.getString("INDEX_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(quest)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("idx_func_single", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(colour)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("idx_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertTrue(rs.getBoolean("NON_UNIQUE"));

    assertTrue(!rs.next());

    rs.close();
  }

  @Test
  public void testNotNullDomainColumn() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "domaintable", "");
    assertTrue(rs.next());
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(Types.DISTINCT, rs.getInt("DATA_TYPE"));
    assertEquals("nndom", rs.getString("TYPE_NAME"));
    assertEquals(Types.INTEGER, rs.getInt("SOURCE_DATA_TYPE"));
    assertEquals("NO", rs.getString("IS_NULLABLE"));
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testAscDescIndexInfo() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("CREATE INDEX idx_a_d ON metadatatest (id ASC, quest DESC)");
    stmt.close();

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next());
    assertEquals("idx_a_d", rs.getString("INDEX_NAME"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals("A", rs.getString("ASC_OR_DESC"));

    assertTrue(rs.next());
    assertEquals("idx_a_d", rs.getString("INDEX_NAME"));
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals("D", rs.getString("ASC_OR_DESC"));

    rs.close();
  }

  @Test
  public void testPartialIndexInfo() throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("create index idx_p_name_id on metadatatest (name) where id > 5");
    stmt.close();

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next());
    assertEquals("idx_p_name_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("name", rs.getString("COLUMN_NAME"));
    assertEquals("(id > 5)", rs.getString("FILTER_CONDITION"));
    assertTrue(rs.getBoolean("NON_UNIQUE"));

    rs.close();
  }

  @Test
  public void testTableTypes() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getTableTypes();
    rs.close();
  }

  @Test
  public void testFuncWithoutNames() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getProcedureColumns(null, null, "f1", null);

    assertTrue(rs.next());
    assertEquals("returnValue", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnReturn, rs.getInt(5));

    assertTrue(rs.next());
    assertEquals("$1", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("$2", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(!rs.next());

    rs.close();
  }

  @Test
  public void testFuncWithNames() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getProcedureColumns(null, null, "f2", null);

    assertTrue(rs.next());

    assertTrue(rs.next());
    assertEquals("a", rs.getString(4));

    assertTrue(rs.next());
    assertEquals("b", rs.getString(4));

    assertTrue(!rs.next());

    rs.close();
  }

  @Test
  public void testFuncWithDirection() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getProcedureColumns(null, null, "f3", null);

    assertTrue(rs.next());
    assertEquals("a", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("b", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnInOut, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("c", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnOut, rs.getInt(5));
    assertEquals(Types.TIMESTAMP, rs.getInt(6));

    rs.close();
  }

  @Test
  public void testFuncReturningComposite() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getProcedureColumns(null, null, "f4", null);

    assertTrue(rs.next());
    assertEquals("$1", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("id", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("name", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("updated", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.TIMESTAMP, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("colour", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next());
    assertEquals("quest", rs.getString(4));
    assertEquals(DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testVersionColumns() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getVersionColumns(null, null, "pg_class");
    rs.close();
  }

  @Test
  public void testBestRowIdentifier() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getBestRowIdentifier(null, null, "pg_type", DatabaseMetaData.bestRowSession, false);
    rs.close();
  }

  @Test
  public void testProcedures() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    ResultSet rs = dbmd.getProcedures(null, null, null);
    rs.close();
  }

  @Test
  public void testCatalogs() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getCatalogs();
    assertTrue(rs.next());
    assertEquals(con.getCatalog(), rs.getString(1));
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testSchemas() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    ResultSet rs = dbmd.getSchemas();
    boolean foundPublic = false;
    boolean foundEmpty = false;
    boolean foundPGCatalog = false;
    int count;

    for (count = 0; rs.next(); count++) {
      String schema = rs.getString("TABLE_SCHEM");
      if ("public".equals(schema)) {
        foundPublic = true;
      }
      else if ("".equals(schema)) {
        foundEmpty = true;
      }
      else if ("pg_catalog".equals(schema)) {
        foundPGCatalog = true;
      }
    }
    rs.close();
    assertTrue(count >= 2);
    assertTrue(foundPublic);
    assertTrue(foundPGCatalog);
    assertTrue(!foundEmpty);
  }

  @Test
  public void testEscaping() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getTables(null, null, "a'", new String[] {"TABLE"});
    assertTrue(rs.next());
    rs.close();
    rs = dbmd.getTables(null, null, "a\\\\", new String[] {"TABLE"});
    assertTrue(rs.next());
    rs.close();
    rs = dbmd.getTables(null, null, "a\\", new String[] {"TABLE"});
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testSearchStringEscape() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();
    String pattern = dbmd.getSearchStringEscape() + "_";
    PreparedStatement pstmt = con.prepareStatement("SELECT 'a' LIKE ?, '_' LIKE ?");
    pstmt.setString(1, pattern);
    pstmt.setString(2, pattern);
    ResultSet rs = pstmt.executeQuery();
    assertTrue(rs.next());
    assertTrue(!rs.getBoolean(1));
    assertTrue(rs.getBoolean(2));
    rs.close();
    pstmt.close();
  }

  @Test
  public void testGetUDTQualified() throws Exception {
    Statement stmt = null;
    try {
      stmt = con.createStatement();
      stmt.execute("create schema jdbc");
      stmt.execute("create type jdbc.testint8 as (i int8)");
      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getUDTs(null, null, "jdbc.testint8", null);
      assertTrue(rs.next());

      @SuppressWarnings("unused")
      String cat, schema, typeName, remarks, className;

      @SuppressWarnings("unused")
      int dataType, baseType;

      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");
      baseType = rs.getInt("base_type");
      assertEquals("type name ", "testint8", typeName);
      assertEquals("schema name ", "jdbc", schema);

      rs.close();

      // now test to see if the fully qualified stuff works as planned
      rs = dbmd.getUDTs("catalog", "public", "catalog.jdbc.testint8", null);
      assertTrue(rs.next());
      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");
      baseType = rs.getInt("base_type");
      assertEquals("type name ", "testint8", typeName);
      assertEquals("schema name ", "jdbc", schema);

      rs.close();
    }
    finally {
      try {
        if (stmt != null)
          stmt.close();
        stmt = con.createStatement();
        stmt.execute("drop type jdbc.testint8");
        stmt.execute("drop schema jdbc");
        stmt.close();
      }
      catch (Exception ex) {
        // Ignore
      }
    }

  }

  @Test
  public void testGetUDT1() throws Exception {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      stmt.close();

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getUDTs(null, null, "testint8", null);
      assertTrue(rs.next());

      @SuppressWarnings("unused")
      String cat, schema, typeName, remarks, className;

      @SuppressWarnings("unused")
      int dataType, baseType;

      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");

      baseType = rs.getInt("base_type");
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

      rs.close();
    }
    finally {
      try {
        Statement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
        stmt.close();
      }
      catch (Exception ex) {
        // Ignore
      }
    }
  }

  @Test
  public void testGetUDT2() throws Exception {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      stmt.close();

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getUDTs(null, null, "testint8", new int[] {Types.DISTINCT, Types.STRUCT});
      assertTrue(rs.next());

      @SuppressWarnings("unused")
      String cat, schema, typeName, remarks, className;

      @SuppressWarnings("unused")
      int dataType, baseType;

      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");

      baseType = rs.getInt("base_type");
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

      rs.close();
    }
    finally {
      try {
        Statement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
        stmt.close();
      }
      catch (Exception ex) {
        // Ignore
      }
    }
  }

  @Test
  public void testGetUDT3() throws Exception {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      stmt.close();

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getUDTs(null, null, "testint8", new int[] {Types.DISTINCT});
      assertTrue(rs.next());

      @SuppressWarnings("unused")
      String cat, schema, typeName, remarks, className;

      @SuppressWarnings("unused")
      int dataType, baseType;

      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");

      baseType = rs.getInt("base_type");
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

      rs.close();
    }
    finally {
      try {
        Statement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
        stmt.close();
      }
      catch (Exception ex) {
        // Ignore
      }
    }
  }

  @Test
  public void testGetUDT4() throws Exception {
    try {
      Statement stmt = con.createStatement();
      stmt.execute("create type testint8 as (i int8)");
      stmt.close();

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getUDTs(null, null, "testint8", null);
      assertTrue(rs.next());

      @SuppressWarnings("unused")
      String cat, schema, typeName, remarks, className;

      @SuppressWarnings("unused")
      int dataType, baseType;

      cat = rs.getString("type_cat");
      schema = rs.getString("type_schem");
      typeName = rs.getString("type_name");
      className = rs.getString("class_name");
      dataType = rs.getInt("data_type");
      remarks = rs.getString("remarks");

      baseType = rs.getInt("base_type");
      assertTrue("base type", rs.wasNull());
      assertEquals("data type", Types.STRUCT, dataType);
      assertEquals("type name ", "testint8", typeName);

      rs.close();
    }
    finally {
      try {
        Statement stmt = con.createStatement();
        stmt.execute("drop type testint8");
        stmt.close();
      }
      catch (Exception ex) {
        // Ignore
      }
    }
  }

  @Test
  public void testAttributes() throws SQLException {

    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getAttributes(null, null, "attr_test", null);

    assertTrue(rs.next());
    assertEquals("public", rs.getString("TYPE_SCHEM"));
    assertEquals("attr_test", rs.getString("TYPE_NAME"));
    assertEquals("val1", rs.getString("ATTR_NAME"));
    assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
    assertEquals("text", rs.getString("ATTR_TYPE_NAME"));
    assertEquals(-1, rs.getInt("ATTR_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertEquals(DatabaseMetaData.attributeNullable, rs.getInt("NULLABLE"));
    assertEquals("this is a attribute comment", rs.getString("REMARKS"));
    assertEquals(null, rs.getString("ATTR_DEF"));
    assertEquals(-1, rs.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("YES", rs.getString("IS_NULLABLE"));
    assertEquals(null, rs.getObject("SOURCE_DATA_TYPE"));

    assertTrue(rs.next());
    assertEquals("public", rs.getString("TYPE_SCHEM"));
    assertEquals("attr_test", rs.getString("TYPE_NAME"));
    assertEquals("val2", rs.getString("ATTR_NAME"));
    assertEquals(Types.NUMERIC, rs.getInt("DATA_TYPE"));
    assertEquals("numeric", rs.getString("ATTR_TYPE_NAME"));
    assertEquals(8, rs.getInt("ATTR_SIZE"));
    assertEquals(3, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertEquals(DatabaseMetaData.attributeNullable, rs.getInt("NULLABLE"));
    assertEquals(null, rs.getString("REMARKS"));
    assertEquals(null, rs.getString("ATTR_DEF"));
    assertEquals(-1, rs.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));
    assertEquals("YES", rs.getString("IS_NULLABLE"));
    assertEquals(null, rs.getObject("SOURCE_DATA_TYPE"));

    assertTrue(rs.next());
    assertEquals("public", rs.getString("TYPE_SCHEM"));
    assertEquals("attr_test", rs.getString("TYPE_NAME"));
    assertEquals("val3", rs.getString("ATTR_NAME"));
    assertEquals(Types.DISTINCT, rs.getInt("DATA_TYPE"));
    assertEquals("nndom", rs.getString("ATTR_TYPE_NAME"));
    assertEquals(10, rs.getInt("ATTR_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertEquals(DatabaseMetaData.attributeNoNulls, rs.getInt("NULLABLE"));
    assertEquals(null, rs.getString("REMARKS"));
    assertEquals(null, rs.getString("ATTR_DEF"));
    assertEquals(4, rs.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(3, rs.getInt("ORDINAL_POSITION"));
    assertEquals("NO", rs.getString("IS_NULLABLE"));
    assertEquals(Types.INTEGER, rs.getShort("SOURCE_DATA_TYPE"));

    rs.close();
  }

  @Test
  public void testTypeInfoSigned() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getTypeInfo();
    while (rs.next()) {
      if ("int4".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(false, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      }
      else if ("float8".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(false, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      }
      else if ("text".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(true, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      }
    }
    rs.close();
  }

  @Test
  public void testTypeInfoQuoting() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getTypeInfo();
    while (rs.next()) {
      if ("int4".equals(rs.getString("TYPE_NAME"))) {
        assertNull(rs.getString("LITERAL_PREFIX"));
      }
      else if ("text".equals(rs.getString("TYPE_NAME"))) {
        assertEquals("'", rs.getString("LITERAL_PREFIX"));
        assertEquals("'", rs.getString("LITERAL_SUFFIX"));
      }
    }
    rs.close();
  }

  @Test
  public void testInformationAboutArrayTypes() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "arraytable", "");
    assertTrue(rs.next());
    assertEquals("a", rs.getString("COLUMN_NAME"));
    assertEquals(5, rs.getInt("COLUMN_SIZE"));
    assertEquals(2, rs.getInt("DECIMAL_DIGITS"));
    assertTrue(rs.next());
    assertEquals("b", rs.getString("COLUMN_NAME"));
    assertEquals(100, rs.getInt("COLUMN_SIZE"));
    assertTrue(!rs.next());
    rs.close();
  }

  @Test
  public void testGetClientInfoProperties() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();

    ResultSet rs = dbmd.getClientInfoProperties();

    assertTrue(rs.next());
    assertEquals("ApplicationName", rs.getString("NAME"));

    rs.close();
  }

  @Test
  public void testGetColumnsForAutoIncrement() throws Exception {
    DatabaseMetaData dbmd = con.getMetaData();

    ResultSet rs = dbmd.getColumns("%", "%", "sercoltest", "%");
    assertTrue(rs.next());
    assertEquals("a", rs.getString("COLUMN_NAME"));
    assertEquals("NO", rs.getString("IS_AUTOINCREMENT"));

    assertTrue(rs.next());
    assertEquals("b", rs.getString("COLUMN_NAME"));
    assertEquals("YES", rs.getString("IS_AUTOINCREMENT"));

    assertTrue(rs.next());
    assertEquals("c", rs.getString("COLUMN_NAME"));
    assertEquals("YES", rs.getString("IS_AUTOINCREMENT"));

    assertTrue(!rs.next());

    rs.close();
  }

  @Test
  public void testGetSchemas() throws SQLException {
    DatabaseMetaData dbmd = con.getMetaData();

    ResultSet rs = dbmd.getSchemas("", "publ%");

    assertTrue(rs.next());
    assertEquals("public", rs.getString("TABLE_SCHEM"));
    assertNull(rs.getString("TABLE_CATALOG"));
    assertTrue(!rs.next());

    rs.close();
  }

}
