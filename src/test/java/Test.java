
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.impossibl.postgres.jdbc.PSQLConnection;
import com.impossibl.postgres.jdbc.PSQLDriver;


public class Test {

	public static class RoleGrant {

		public UUID account_id;
		public String role;

	}

	public static class Helper2 {

		public String node;
		public RoleGrant rg;

	}

	public static void main(String[] args) throws SQLException, IOException {

		Properties settings = new Properties();
		settings.put("database", "impossibl");
		settings.put("username", "postgres");
		settings.put("password", "test");

		Map<String, Class<?>> targetTypeMap = new HashMap<String, Class<?>>();
		targetTypeMap.put("helper2", Helper2.class);
		targetTypeMap.put("role_granter", RoleGrant.class);
		
		PSQLDriver driver = new PSQLDriver();
		
		// Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl", settings);

		PSQLConnection conn = driver.connect("jdbc:postgresql://mongo/impossibl", settings);
		conn.setTypeMap(targetTypeMap);
		
		conn.prepareStatement("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']").execute();
		conn.prepareStatement("select ('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2").execute();
		conn.prepareStatement("select oid,typname,typlen,typbyval,typcategory,typdelim,typrelid from pg_type where oid=$1").execute();
	}
	
}
