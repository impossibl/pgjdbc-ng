
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.impossibl.postgres.jdbc.PGSQLOutput;


public class Test {

	public static class RoleGrant implements SQLData {

		public UUID account_id;
		public String role;
		
		@Override
		public String getSQLTypeName() throws SQLException {
			return "role_grant";
		}
		
		@Override
		public void readSQL(SQLInput stream, String typeName) throws SQLException {
			account_id = (UUID)stream.readObject();
			role = stream.readString();
		}
		
		@Override
		public void writeSQL(SQLOutput stream) throws SQLException {
			((PGSQLOutput)stream).writeObject(account_id);
			stream.writeString(role);			
		}

	}

	public static class Helper2 implements SQLData {

		public String node;
		public RoleGrant rg;
		
		@Override
		public String getSQLTypeName() throws SQLException {
			return "helper2";
		}
		
		@Override
		public void readSQL(SQLInput stream, String typeName) throws SQLException {
			node = stream.readString();
			rg = (RoleGrant) stream.readObject();
		}
		
		@Override
		public void writeSQL(SQLOutput stream) throws SQLException {
			stream.writeString(node);
			stream.writeObject(rg);
		}

	}

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InterruptedException {
		
		try(Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl?user=postgres&password=test")) {

			test(conn);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	static void test(Connection conn) throws SQLException, FileNotFoundException {
		
//		conn.setAutoCommit(false);

//		PreparedStatement ps = conn.prepareStatement("insert into blob_test values(?)");
//		ps.setBlob(1, new FileInputStream(new File("/Users/kdubb/Downloads/test.pdf")), 50*1024);
//		ps.executeUpdate();
//		
//		conn.commit();
		
//		DatabaseMetaData dmd = conn.getMetaData();
//		dmd.getProcedures(null, null, null);
//		dmd.getProcedureColumns(null, null, null, null);
//		dmd.getTables(null, null, null, null);
//		dmd.getSchemas(null, null);
//		dmd.getCatalogs();
//		dmd.getTableTypes();
//		dmd.getColumns(null, null, null, null);
//		dmd.getColumns(null, "public", "%\\_test", "%");
//		dmd.getPrimaryKeys(null, "public", "%\\_test");
//		dmd.getImportedKeys(null, "public", "%\\_test");
//		dmd.getExportedKeys(null, "public", "%\\_test");
//		dmd.getCrossReference(null, "public", "%\\_test", null, "public", "%\\_test");
//		dmd.getTypeInfo();
		
		//Logger.getLogger("com.impossibl.postgres").setLevel(ALL);

//		for(int i=0; i < 10; i++) {
//		
//			new Thread() {
//				
//				public void run() {
//					
//					try {
//						Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl?username=postgres&password=test");
//			
//						for(int c=0; c < 100; ++c) {
//							Timer timer = new Timer();
//							PreparedStatement ps0 = conn.prepareStatement("select val from big_test limit 10");
//							ps0.setMaxFieldSize(5);
//							ResultSet rs0 = ps0.executeQuery();
//							while(rs0.next()) {
//								System.out.println(rs0.getObject(1));
//							}
//							rs0.close();
//							ps0.close();
//							System.out.println("SELECT TIME = " + timer.getLap());
//						}
//						
//						conn.close();
//					}
//					catch(SQLException e) {
//						e.printStackTrace();
//					}
//				}
//				
//			}.start();
//			
//		}
//
		
		conn.isValid(0);
		
		Map<String, Class<?>> targetTypeMap = new HashMap<String, Class<?>>();
		targetTypeMap.put("helper2", Helper2.class);
		targetTypeMap.put("role_granter", RoleGrant.class);
		
		//conn.setTypeMap(targetTypeMap);

		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select bits from bit_test");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				System.out.println(rs0.getObject(1));
//			}
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select num from num_test");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				System.out.println(rs0.getObject(1));
//			}
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-06 19:15:25.514776'::timestamp");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				System.out.println(rs0.getObject(1));
//			}
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-06 19:15:25.514776'::timestamptz");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				System.out.println(rs0.getObject(1));
//			}
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("insert into dt_test (d) values (?)");
//			ps0.setDate(1, new Date(System.currentTimeMillis()));
//			System.out.println("INSERTED " + ps0.executeUpdate() + " rows");
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-07'::date");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				System.out.println(rs0.getObject(1));
//			}
//		}
//		
//		{
//			PreparedStatement ps0 = conn.prepareStatement("select d,t,ttz,ts from dt_test");
//			ResultSet rs0 = ps0.executeQuery();
//			while(rs0.next()) {
//				for(int c=1; c < 5; ++c) {
//					System.out.print(rs0.getObject(c));
//					System.out.print(", ");
//				}
//				System.out.println();
//			}
//		}
//		
//		PreparedStatement ps1 = conn.prepareStatement("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']");
//		ResultSet rs1 = ps1.executeQuery();
//		while(rs1.next()) {
//			System.out.println(rs1.getObject(1));
//		}
//		
//		PreparedStatement ps2 = conn.prepareStatement("select ('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2");
//		ResultSet rs2 = ps2.executeQuery();
//		while(rs2.next()) {
//			System.out.println(rs2.getObject(1));
//		}

		Statement s0 = conn.createStatement();
		
		s0.executeUpdate("insert into num_test(num) values (948097334.33344234)", new String[] {"num, spval"});
		try(ResultSet rs = s0.getGeneratedKeys()) {
			
			ResultSetMetaData md = rs.getMetaData();
			for(int c=0; c < md.getColumnCount(); ++c) {				
				System.out.print(md.getCatalogName(c+1) + ", ");
				System.out.print(md.getSchemaName(c+1) + ", ");
				System.out.print(md.getTableName(c+1) + ", ");
				System.out.print(md.getColumnName(c+1) + ", ");
				System.out.print(md.getColumnLabel(c+1) + ", ");
				System.out.print(md.getColumnType(c+1) + ", ");
				System.out.print(md.getColumnTypeName(c+1) + ", ");
				System.out.print(md.getPrecision(c+1) + ", ");
				System.out.print(md.getScale(c+1) + ", ");
				System.out.print(md.getColumnDisplaySize(c+1) + ", ");
				System.out.print(md.isNullable(c+1) + ", ");
				System.out.print(md.isAutoIncrement(c+1) + ", ");
				System.out.print(md.isSigned(c+1) + ", ");				
				System.out.print(md.isCurrency(c+1) + ", ");
				System.out.print(md.isCaseSensitive(c+1) + ", ");
				System.out.println();
			}
			System.out.println();
			
			while(rs.next()) {
				System.out.print(rs.getObject(1));
			}
			System.out.println();
		}
		
		try(PreparedStatement ps1 = conn.prepareStatement("insert into dt_test(ts) values (?)", new String[] {"ts"})) {
			ps1.setString(1, "10-11-2008 12:44");
			ps1.execute();
			try(ResultSet rs = ps1.getGeneratedKeys()) {
				while(rs.next()) {
					System.out.println(rs.getObject(1));
				}
			}
		}
		
		try(PreparedStatement ps1 = conn.prepareStatement("insert into dt_test(d) values (?)", new String[] {"d"})) {
			//ps1.setString(1, "December 31, 1999");
			ps1.setString(1, "12-31-1999");
			ps1.execute();
			try(ResultSet rs = ps1.getGeneratedKeys()) {
				while(rs.next()) {
					System.out.println(rs.getObject(1));
				}
			}
		}
		ResultSet rs0 = s0.executeQuery("select bits from bit_test");
		while(rs0.next()) {
			System.out.println(rs0.getObject(1));
		}

			rs0 = s0.executeQuery("select num from num_test");
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}
		
			rs0 = s0.executeQuery("select '2013-03-06 19:15:25.514776'::timestamp");
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}
		
			rs0 = s0.executeQuery("select '2013-03-06 19:15:25.514776'::timestamptz");
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}

			rs0 = s0.executeQuery("select '2013-03-07'::date");
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}
		
			rs0 = s0.executeQuery("select d,t,ttz,ts from dt_test");
			while(rs0.next()) {
				for(int c=1; c < 5; ++c) {
					System.out.print(rs0.getObject(c));
					System.out.print(", ");
				}
				System.out.println();
			}
		
		rs0 = s0.executeQuery("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']");
		while(rs0.next()) {
			System.out.println(rs0.getObject(1));
		}
		
		rs0 = s0.executeQuery("select array[('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2,('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2]");
		while(rs0.next()) {
			ResultSet arrayRs = rs0.getArray(1).getResultSet();
			arrayRs.next();
			System.out.println(arrayRs.getObject(2, targetTypeMap));
		}
		
		conn.setTypeMap(targetTypeMap);
		
		rs0.first();
		try(PreparedStatement ps1 = conn.prepareStatement("select ?::helper2[]")) {
			ps1.setObject(1, rs0.getObject(1, targetTypeMap));
			rs0 = ps1.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}
		}

		rs0 = s0.executeQuery("select ('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2");
		while(rs0.next()) {
			System.out.println(rs0.getObject(1));
		}
		
		rs0.first();
		try(PreparedStatement ps1 = conn.prepareStatement("select ?::helper2")) {
			ps1.setObject(1, rs0.getObject(1));
			rs0 = ps1.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(1));
			}
		}
		
		conn.close();
	}
	
}
