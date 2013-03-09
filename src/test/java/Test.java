
import static java.util.logging.Level.ALL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Logger;


public class Test {

	public static class RoleGrant {

		public UUID account_id;
		public String role;

	}

	public static class Helper2 {

		public String node;
		public RoleGrant rg;

	}

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		
		for(Handler x : Logger.getLogger("").getHandlers()) {
			x.setLevel(ALL);
		}
		
		Logger.getLogger("com.impossibl.postgres").setLevel(ALL);
		
		Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl?username=postgres&password=test");

		Map<String, Class<?>> targetTypeMap = new HashMap<String, Class<?>>();
		targetTypeMap.put("helper2", Helper2.class);
		targetTypeMap.put("role_granter", RoleGrant.class);
		
		conn.setTypeMap(targetTypeMap);

		{
			PreparedStatement ps0 = conn.prepareStatement("select m from money_test");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select bits from bit_test");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select num from num_test");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-06 19:15:25.514776'::timestamp");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-06 19:15:25.514776'::timestamptz");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("insert into dt_test (d) values (?)");
			ps0.setDate(0, new Date(System.currentTimeMillis()));
			System.out.println("INSERTED " + ps0.executeUpdate() + " rows");
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select '2013-03-07'::date");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				System.out.println(rs0.getObject(0));
			}
		}
		
		{
			PreparedStatement ps0 = conn.prepareStatement("select d,t,ttz,ts from dt_test");
			ResultSet rs0 = ps0.executeQuery();
			while(rs0.next()) {
				for(int c=0; c < 4; ++c) {
					System.out.print(rs0.getObject(c));
					System.out.print(", ");
				}
				System.out.println();
			}
		}
		
		PreparedStatement ps1 = conn.prepareStatement("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']");
		ResultSet rs1 = ps1.executeQuery();
		while(rs1.next()) {
			System.out.println(rs1.getObject(0));
		}
		
		PreparedStatement ps2 = conn.prepareStatement("select ('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2");
		ResultSet rs2 = ps2.executeQuery();
		while(rs2.next()) {
			System.out.println(rs2.getObject(0));
		}
		
	}
	
}
