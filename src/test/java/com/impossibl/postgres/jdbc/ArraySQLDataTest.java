package com.impossibl.postgres.jdbc;

import static org.junit.Assert.*;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArraySQLDataTest {

	static class PersonName implements SQLData {
		public String first, middle, last;

		@Override
		public String getSQLTypeName() throws SQLException {
			return "person_name";
		}

		@Override
		public void readSQL(final SQLInput stream, final String typeName) throws SQLException {
			this.first = stream.readString();
			this.middle = stream.readString();
			this.last = stream.readString();
		}

		@Override
		public void writeSQL(final SQLOutput stream) throws SQLException {
			stream.writeString(first);
			stream.writeString(middle);
			stream.writeString(last);
		}
	}

	static class Point implements SQLData {
		public double x, y;

		@Override
		public String getSQLTypeName() throws SQLException {
			return "point";
		}

		@Override
		public void readSQL(final SQLInput stream, final String typeName) throws SQLException {
			this.x = stream.readDouble();
			this.y = stream.readDouble();
		}

		@Override
		public void writeSQL(final SQLOutput stream) throws SQLException {
			stream.writeDouble(x);
			stream.writeDouble(y);
		}
	}

	Connection con = null;

	@Before
	public void before() throws Exception {
		con = TestUtil.openDB();
		TestUtil.createTempTable(con, "test_sqldata", "n text[], p point");

	}

	@After
	public void after() throws Exception {
		TestUtil.dropTable(con, "test_sqldata");
		con.close();
	}

	private ResultSet doTest(final PersonName n, final Point p) throws SQLException {
		try (final PreparedStatement insert = con.prepareStatement("insert into test_sqldata(n, p) values (?, ?);")) {
			insert.setObject(1, n);
			insert.setObject(2, p);

			final int rows = insert.executeUpdate();

			assertEquals(1, rows);

			insert.close();

			final PreparedStatement select = con.prepareStatement("select n, p from test_sqldata");

			final ResultSet rs = select.executeQuery();
			assertTrue(rs.next());
			return rs;

		}
	}

	@Test
	public void testFull() throws SQLException {

		final PersonName n = new PersonName();
		n.first = "John";
		n.middle = "Q";
		n.last = "Doe";

		final Point p = new Point();
		p.x = -2.5;
		p.y = 3.14;

		try (final ResultSet rs = doTest(n, p)) {
			final Object o1 = rs.getObject(1);
			final PersonName eN = rs.getObject(1, PersonName.class);
			assertNotNull(o1);
			assertTrue(o1.getClass().isArray());
			assertEquals(3, Array.getLength(o1));
			assertEquals(String.class, o1.getClass().getComponentType());
			assertArrayEquals(new String[] { n.first, n.middle, n.last }, (String[]) o1);

			assertNotNull(eN);
			assertEquals(n.first, eN.first);
			assertEquals(n.middle, eN.middle);
			assertEquals(n.last, eN.last);

			final Object o2 = rs.getObject(2);
			final Point eP = rs.getObject(2, Point.class);
			assertNotNull(o2);
			assertTrue(o2.getClass().isArray());
			assertEquals(2, Array.getLength(o2));
			assertEquals(double.class, o2.getClass().getComponentType());
			assertEquals(p.x, Array.getDouble(o2, 0), 0.00000001);
			assertEquals(p.y, Array.getDouble(o2, 1), 0.00000001);

			assertNotNull(eP);
			assertEquals(p.x, eP.x, 0.00000001);
			assertEquals(p.y, eP.y, 0.00000001);
		}
	}

	@Test
	public void testNullValues() throws SQLException {
		try (final ResultSet rs = doTest(null, null)) {
			final Object o1 = rs.getObject(1);
			final PersonName n = rs.getObject(1, PersonName.class);
			assertNull(o1);
			assertNull(n);

			final Object o2 = rs.getObject(2);
			final Point p = rs.getObject(2, Point.class);
			assertNull(o2);
			assertNull(p);
		}
	}

	@Test
	public void testNullComponent() throws SQLException {
		final PersonName n = new PersonName();
		n.first = null;
		n.middle = "Q";
		n.last = "Doe";

		try (final ResultSet rs = doTest(n, null)) {
			final Object o1 = rs.getObject(1);
			final PersonName eN = rs.getObject(1, PersonName.class);
			assertNotNull(o1);
			assertTrue(o1.getClass().isArray());
			assertEquals(3, Array.getLength(o1));
			assertEquals(String.class, o1.getClass().getComponentType());
			assertArrayEquals(new String[] { n.first, n.middle, n.last }, (String[]) o1);

			assertNotNull(eN);
			assertEquals(n.first, eN.first);
			assertEquals(n.middle, eN.middle);
			assertEquals(n.last, eN.last);
		}
	}

}
