package com.impossibl.postgres.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StructTest {
	
	static class TestStruct implements SQLData {
		
		String str;
		String str2;
		UUID id;
		Double num;

		@Override
		public String getSQLTypeName() throws SQLException {
			return "teststruct";
		}

		@Override
		public void readSQL(SQLInput stream, String typeName) throws SQLException {
			str = stream.readString();
			str2 = stream.readString();
			id = (UUID) stream.readObject();
			num = stream.readDouble();
		}

		@Override
		public void writeSQL(SQLOutput stream) throws SQLException {
			stream.writeString(str);
			stream.writeString(str2);
			((PGSQLOutput)stream).writeObject(id);
			stream.writeDouble(num);
		}
		
	}
	
	static Connection conn;
	

	@Before
	public void setUp() throws Exception {
		conn = TestUtil.openDB();
		TestUtil.createType(conn, "teststruct" , "str text, str2 text, id uuid, num float");
		TestUtil.createTable(conn, "struct_test", "val teststruct");
	}

	@After
	public void tearDown() throws Exception {
		TestUtil.dropTable(conn, "struct_test");
		TestUtil.dropType(conn, "teststruct");
		TestUtil.closeDB(conn);
	}
	
	@Test
	public void testSpecificType() throws SQLException {
		
		TestStruct ts = new TestStruct(), ts2;
		ts.id = UUID.randomUUID();
		ts.num = new Random().nextDouble();
		ts.str = "!}({%*}{%}{(%&}{%^}{&";
		ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";
		
		PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
		pst.setObject(1, ts);
		pst.executeUpdate();
		pst.close();

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
		assertTrue(rs.next());
		assertNotNull(ts2 = rs.getObject(1, TestStruct.class));
		assertEquals(ts.str, ts2.str);
		assertEquals(ts.str2, ts2.str2);
		assertEquals(ts.id, ts2.id);
		assertEquals(ts.num, ts2.num, 0.00000001);
	}

	@Test
	public void testResultSetTypeMap() throws SQLException {
		
		Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
		typeMap.put("teststruct", TestStruct.class);
		
		TestStruct ts = new TestStruct(), ts2;
		ts.id = UUID.randomUUID();
		ts.num = new Random().nextDouble();
		ts.str = "!}({%*}{%}{(%&}{%^}{&";
		ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";
		
		PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
		pst.setObject(1, ts);
		pst.executeUpdate();
		pst.close();

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
		assertTrue(rs.next());
		assertNotNull(ts2 = (TestStruct) rs.getObject(1, typeMap));
		assertEquals(ts.str, ts2.str);
		assertEquals(ts.str2, ts2.str2);
		assertEquals(ts.id, ts2.id);
		assertEquals(ts.num, ts2.num, 0.00000001);
		
	}

	@Test
	public void testConnectionTypeMap() throws SQLException {
		
		Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
		typeMap.put("teststruct", TestStruct.class);
		
		conn.setTypeMap(typeMap);
		
		TestStruct ts = new TestStruct(), ts2;
		ts.id = UUID.randomUUID();
		ts.num = new Random().nextDouble();
		ts.str = "!}({%*}{%}{(%&}{%^}{&";
		ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";
		
		PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
		pst.setObject(1, ts);
		pst.executeUpdate();
		pst.close();

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
		assertTrue(rs.next());
		assertNotNull(ts2 = (TestStruct) rs.getObject(1, typeMap));
		assertEquals(ts.str, ts2.str);
		assertEquals(ts.str2, ts2.str2);
		assertEquals(ts.id, ts2.id);
		assertEquals(ts.num, ts2.num, 0.00000001);
		
	}

	@Test
	public void testConnectionTypeMapFail() throws SQLException {
		
		@SuppressWarnings("unused")
		TestStruct ts = new TestStruct(), ts2;
		ts.id = UUID.randomUUID();
		ts.num = new Random().nextDouble();
		ts.str = "!}({%*}{%}{(%&}{%^}{&";
		ts.str2 = "!}({%*}{%}{(%&}{%^}][][]'\"\"\"][]'\"}{}['{&";
		
		PreparedStatement pst = conn.prepareStatement("INSERT INTO struct_test VALUES (?)");
		pst.setObject(1, ts);
		pst.executeUpdate();
		pst.close();

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM struct_test; SELECT 1;");
		assertTrue(rs.next());

		try {
			ts2 = (TestStruct) rs.getObject(1);
			Assert.fail("Cast should have failed");
		}
		catch(ClassCastException e) {
			//Should fail
		}
		finally {
			rs.close();
			st.close();
		}
		
	}

}
