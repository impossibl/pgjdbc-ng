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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import junit.framework.TestCase;



/*
 * TestCase to test the internal functionality of
 * org.postgresql.jdbc2.DatabaseMetaData's various properties.
 * Methods which return a ResultSet are tested elsewhere.
 * This avoids a complicated setUp/tearDown for something like
 * assertTrue(dbmd.nullPlusNonNullIsNull());
 */

public class DatabaseMetaDataPropertiesTest extends TestCase {

	private Connection con;

	/*
	 * Constructor
	 */
	public DatabaseMetaDataPropertiesTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		con = TestUtil.openDB();
	}

	protected void tearDown() throws Exception {
		TestUtil.closeDB(con);
	}

	/*
	 * The spec says this may return null, but we always do!
	 */
	public void testGetMetaData() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);
	}

	/*
	 * Test default capabilities
	 */
	public void testCapabilities() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(dbmd.allProceduresAreCallable());
		assertTrue(dbmd.allTablesAreSelectable()); // not true all the time

		// This should always be false for postgresql (at least for 7.x)
		assertTrue(!dbmd.isReadOnly());

		// we support multiple resultsets via multiple statements in one execute()
		// now
		assertTrue(dbmd.supportsMultipleResultSets());

		// yes, as multiple backends can have transactions open
		assertTrue(dbmd.supportsMultipleTransactions());

		assertTrue(dbmd.supportsMinimumSQLGrammar());
		assertTrue(dbmd.supportsCoreSQLGrammar());
		assertTrue(!dbmd.supportsExtendedSQLGrammar());
		assertTrue(dbmd.supportsANSI92EntryLevelSQL());
		assertTrue(!dbmd.supportsANSI92IntermediateSQL());
		assertTrue(!dbmd.supportsANSI92FullSQL());

		assertTrue(dbmd.supportsIntegrityEnhancementFacility());

	}

	public void testJoins() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(dbmd.supportsOuterJoins());
		assertTrue(dbmd.supportsFullOuterJoins());
		assertTrue(dbmd.supportsLimitedOuterJoins());
	}

	public void testCursors() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

//TODO: reconcile against mainstream driver
//		assertTrue(!dbmd.supportsPositionedDelete());
//		assertTrue(!dbmd.supportsPositionedUpdate());
	}

	public void testValues() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);
		int indexMaxKeys = dbmd.getMaxColumnsInIndex();
		assertEquals(32, indexMaxKeys);
	}

	public void testNulls() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(!dbmd.nullsAreSortedAtStart());
		assertTrue(!dbmd.nullsAreSortedAtEnd());
		assertTrue(dbmd.nullsAreSortedHigh());
		assertTrue(!dbmd.nullsAreSortedLow());

		assertTrue(dbmd.nullPlusNonNullIsNull());

		assertTrue(dbmd.supportsNonNullableColumns());
	}

	public void testLocalFiles() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(!dbmd.usesLocalFilePerTable());
		assertTrue(!dbmd.usesLocalFiles());
	}

	public void testIdentifiers() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(!dbmd.supportsMixedCaseIdentifiers()); // always false
		assertTrue(dbmd.supportsMixedCaseQuotedIdentifiers()); // always true

		assertTrue(!dbmd.storesUpperCaseIdentifiers()); // always false
		assertTrue(dbmd.storesLowerCaseIdentifiers()); // always true
		assertTrue(!dbmd.storesUpperCaseQuotedIdentifiers()); // always false
		assertTrue(!dbmd.storesLowerCaseQuotedIdentifiers()); // always false
		assertTrue(!dbmd.storesMixedCaseQuotedIdentifiers()); // always false

		assertTrue(dbmd.getIdentifierQuoteString().equals("\""));

	}

	public void testTables() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		// we can add columns
		assertTrue(dbmd.supportsAlterTableWithAddColumn());

		// we can drop columns
		assertTrue(dbmd.supportsAlterTableWithDropColumn());
	}

	public void testSelect() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		// yes we can?: SELECT col a FROM a;
		assertTrue(dbmd.supportsColumnAliasing());

		// yes we can have expressions in ORDERBY
		assertTrue(dbmd.supportsExpressionsInOrderBy());

		// Yes, an ORDER BY clause can contain columns that are not in the
		// SELECT clause.
		assertTrue(dbmd.supportsOrderByUnrelated());

		assertTrue(dbmd.supportsGroupBy());
		assertTrue(dbmd.supportsGroupByUnrelated());
		assertTrue(dbmd.supportsGroupByBeyondSelect()); // needs checking
	}

	public void testDBParams() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(dbmd.getURL().equals(TestUtil.getURL()));
		assertTrue(dbmd.getUserName().equals(TestUtil.getUser()));
	}

	public void testDbProductDetails() throws SQLException {
		assertTrue(con instanceof PGConnection);

		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(dbmd.getDatabaseProductName().equals("PostgreSQL"));
	}

	public void testDriverVersioning() throws SQLException {
		DatabaseMetaData dbmd = con.getMetaData();
		assertNotNull(dbmd);

		assertTrue(dbmd.getDriverVersion().equals(PGDriver.VERSION.toString()));
		assertTrue(dbmd.getDriverMajorVersion() == PGDriver.VERSION.getMajor());
		assertTrue(dbmd.getDriverMinorVersion() == PGDriver.VERSION.getMinor());
	}
}
