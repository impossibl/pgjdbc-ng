
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


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

		Properties settings = new Properties();
		settings.put("database", "impossibl");
		settings.put("username", "postgres");
		settings.put("password", "test");

		Map<String, Class<?>> targetTypeMap = new HashMap<String, Class<?>>();
		targetTypeMap.put("helper2", Helper2.class);
		targetTypeMap.put("role_granter", RoleGrant.class);
		
		//PSQLDriver driver = new PSQLDriver();
		//Connection conn = driver.connect("jdbc:postgresql://db/impossibl", settings);
				
		Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl", settings);

		conn.setTypeMap(targetTypeMap);
		
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
		
		PreparedStatement ps3 = conn.prepareStatement("select oid,typname,typlen,typbyval,typcategory,typdelim,typrelid from pg_type");
		ResultSet rs3 = ps3.executeQuery();
		while(rs3.next()) {
			System.out.println(rs3.getObject(0));
		}
		
	}
	
}
