package com.impossibl.postgres.jdbc;

import java.sql.Connection;
import java.sql.Statement;

import junit.framework.TestCase;



/*
 * Test that enhanced error reports return the correct origin
 * for constraint violation errors.
 */
public class ServerErrorTest extends TestCase {

	private Connection con;

	public ServerErrorTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {

		con = TestUtil.openDB();
		
		Statement stmt = con.createStatement();
		stmt.execute("CREATE DOMAIN testdom AS int4 CHECK (value < 10)");

		TestUtil.createTable(con, "testerr", "id int not null, val testdom not null");
		
		stmt.execute("ALTER TABLE testerr ADD CONSTRAINT testerr_pk PRIMARY KEY (id)");
		stmt.close();
	}

	protected void tearDown() throws Exception {
		
		TestUtil.dropTable(con, "testerr");
		
		Statement stmt = con.createStatement();
		stmt.execute("DROP DOMAIN testdom");
		stmt.close();
		
		TestUtil.closeDB(con);
	}

	public void testPrimaryKey() throws Exception {
		
		Statement stmt = con.createStatement();
		stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 1)");
		
		try {
			stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 1)");
			fail("Should have thrown a duplicate key exception.");
		}
		catch (PGSQLException sqle) {
			assertEquals("public", sqle.getSchema());
			assertEquals("testerr", sqle.getTable());
			assertEquals("testerr_pk", sqle.getConstraint());
			assertNull(sqle.getDatatype());
			assertNull(sqle.getColumn());
		}
		
		stmt.close();
	}

	public void testColumn() throws Exception {
		
		Statement stmt = con.createStatement();
		
		try {
			stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, NULL)");
			fail("Should have thrown a not null constraint violation.");
		}
		catch (PGSQLException sqle) {
			assertEquals("public", sqle.getSchema());
			assertEquals("testerr", sqle.getTable());
			assertEquals("val", sqle.getColumn());
			assertNull(sqle.getDatatype());
			assertNull(sqle.getConstraint());
		}
		
		stmt.close();
	}

	public void testDatatype() throws Exception {
		
		Statement stmt = con.createStatement();
		
		try {
			stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 20)");
			fail("Should have thrown a constraint violation.");
		}
		catch (PGSQLException sqle) {
			assertEquals("public", sqle.getSchema());
			assertEquals("testdom", sqle.getDatatype());
			assertEquals("testdom_check", sqle.getConstraint());
		}
		
		stmt.close();
	}

}
