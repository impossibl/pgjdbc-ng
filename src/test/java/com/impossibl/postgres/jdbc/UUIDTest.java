/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2008-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.UUID;

import junit.framework.TestCase;



public class UUIDTest extends TestCase {

	private Connection conn;

	public UUIDTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		conn = TestUtil.openDB();
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TEMP TABLE uuidtest(id uuid)");
		stmt.close();
	}

	protected void tearDown() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.execute("DROP TABLE uuidtest");
		stmt.close();
		TestUtil.closeDB(conn);
	}

	public void testUUID() throws SQLException {
		UUID uuid = UUID.randomUUID();
		PreparedStatement ps = conn.prepareStatement("INSERT INTO uuidtest VALUES (?)");
		ps.setObject(1, uuid, Types.OTHER);
		ps.executeUpdate();
		ps.close();

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT id FROM uuidtest");
		assertTrue(rs.next());

		@SuppressWarnings("unused")
		UUID uuid2 = (UUID) rs.getObject(1);
		assertEquals(uuid, rs.getObject(1));
		assertEquals(uuid.toString(), rs.getString(1));

		rs.close();
		stmt.close();
	}

}
