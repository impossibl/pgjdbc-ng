import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.impossibl.postgres.BasicContext;


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
		
		try (Socket socket = new Socket(InetAddress.getByName("10.0.10.26"), 5432)) {
				
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put("database", "impossibl");
			settings.put("username", "postgres");
			settings.put("password", "test");
			
			Map<String, Class<?>> targetTypeMap = new HashMap<String, Class<?>>();
			targetTypeMap.put("helper2", Helper2.class);
			targetTypeMap.put("role_granter", RoleGrant.class);
			
			BasicContext context = new BasicContext(socket, settings, targetTypeMap);
			context.init();
			
			context.query("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']", HashMap.class);
			context.query("select ('A Name',('cce010e0-8476-11e2-9e96-0800200c9a66'::uuid,'Tester'))::helper2", Object[].class);
			
		}
		
	}
	
}
