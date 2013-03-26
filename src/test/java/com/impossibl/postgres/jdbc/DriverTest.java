/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.TestCase;



/*
 * Tests the dynamically created class PGDriver
 *
 */
public class DriverTest extends TestCase {

	public DriverTest(String name) {
		super(name);
	}

	/*
	 * This tests the acceptsURL() method with a couple of well and poorly formed
	 * jdbc urls.
	 */
	public void testAcceptsURL() throws Exception {
		// Load the driver (note clients should never do it this way!)
		PGDriver drv = new PGDriver();
		assertNotNull(drv);

		// These are always correct
		verifyUrl(drv, "jdbc:postgresql:test", "test", "localhost",5432);
		verifyUrl(drv, "jdbc:postgresql://localhost/test", "test", "localhost",5432);
		verifyUrl(drv, "jdbc:postgresql://localhost:5432/test", "test", "localhost",5432);
		verifyUrl(drv, "jdbc:postgresql://127.0.0.1/anydbname", "anydbname", "127.0.0.1",5432);
		verifyUrl(drv, "jdbc:postgresql://127.0.0.1:5433/hidden", "hidden", "127.0.0.1",5433);
		verifyUrl(drv, "jdbc:postgresql://[::1]:5740/db", "db", "0:0:0:0:0:0:0:1", 5740);

		// Badly formatted url's
		assertTrue(!drv.acceptsURL("jdbc:postgres:test"));
		assertTrue(!drv.acceptsURL("postgresql:test"));
		assertTrue(!drv.acceptsURL("db"));
		assertTrue(!drv.acceptsURL("jdbc:postgresql://localhost:5432a/test"));

		// failover urls
		verifyUrl(drv, "jdbc:postgresql://localhost,127.0.0.1:5432/test", "test", "localhost", 5432, "127.0.0.1", 5432);
		verifyUrl(drv, "jdbc:postgresql://localhost:5433,127.0.0.1:5432/test", "test", "localhost", 5433, "127.0.0.1", 5432);
		verifyUrl(drv, "jdbc:postgresql://[::1],[::1]:5432/db", "db", "0:0:0:0:0:0:0:1", 5432, "0:0:0:0:0:0:0:1", 5432);
		verifyUrl(drv, "jdbc:postgresql://[::1]:5740,127.0.0.1:5432/db", "db", "0:0:0:0:0:0:0:1", 5740, "127.0.0.1", 5432);
	}

	private void verifyUrl(PGDriver drv, String url, String dbName, Object... hosts) throws Exception {
		assertTrue(url, drv.acceptsURL(url));
		PGDriver.ConnectionSpecifier connSpec = drv.parseURL(url);
		assertEquals(url, dbName, connSpec.database);
		assertEquals(url, hosts.length/2, connSpec.addresses.size());
		for(int c=0; c < hosts.length/2; ++c) {
			InetSocketAddress addr = connSpec.addresses.get(c);
			assertEquals(url, hosts[c*2+0], addr.getHostString());
			assertEquals(url, hosts[c*2+1], addr.getPort());
		}
	}

	/*
	 * Tests parseURL (internal)
	 */
	/*
	 * Tests the connect method by connecting to the test database
	 */
	public void testConnect() throws Exception {
		// Test with the url, username & password
		Connection con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
		assertNotNull(con);
		con.close();

		// Test with the username in the url
		con = DriverManager.getConnection(TestUtil.getURL("user",TestUtil.getUser(),"password",TestUtil.getPassword()));
		assertNotNull(con);
		con.close();

		// Test with failover url
		String url = "jdbc:postgresql://invalidhost.not.here," + TestUtil.getServer() + ":" + TestUtil.getPort() + "/" + TestUtil.getDatabase();
		con = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword());
		assertNotNull(con);
		con.close();

	}

	/*
	 * Test that the readOnly property works.
	 */
	public void testReadOnly() throws Exception {
		Connection con = DriverManager.getConnection(TestUtil.getURL("readOnly", true), TestUtil.getUser(), TestUtil.getPassword());
		assertNotNull(con);
		assertTrue(con.isReadOnly());
		con.close();

		con = DriverManager.getConnection(TestUtil.getURL("readOnly", false), TestUtil.getUser(), TestUtil.getPassword());
		assertNotNull(con);
		assertFalse(con.isReadOnly());
		con.close();

		con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
		assertNotNull(con);
		assertFalse(con.isReadOnly());
		con.close();
	}

}
