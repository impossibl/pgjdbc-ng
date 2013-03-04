import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.impossibl.postgres.BasicContext;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.system.tables.PgType;


public class Test {
	
	public static class RoleGrant {
		
		enum Role {
			Tester,
			Manager
		}
		
		public UUID account_id;
		public Role role;
		public UUID getAccount_id() {
			return account_id;
		}
		public void setAccount_id(UUID account_id) {
			this.account_id = account_id;
		}
		public Role getRole() {
			return role;
		}
		public void setRole(Role role) {
			this.role = role;
		}
	}
	
	public static class Helper2 {
		public String node;
		public RoleGrant rg;
		public String getNode() {
			return node;
		}
		public void setNode(String node) {
			this.node = node;
		}
		public RoleGrant getRg() {
			return rg;
		}
		public void setRg(RoleGrant rg) {
			this.rg = rg;
		}
	}

	public static void main(String[] args) throws SQLException, IOException {
		
		try (Socket socket = new Socket(InetAddress.getByName("10.0.10.26"), 5432)) {
				
			Map<String, Object> settings = new HashMap<String, Object>();
			settings.put("database", "impossibl");
			settings.put("username", "postgres");
			settings.put("password", "test");
			
			BasicContext context = new BasicContext(socket, settings);
			context.init();
			
			context.query("select array['A Name','cce010e0-8476-11e2-9e96-0800200c9a66','Tester']", HashMap.class);
		}
		
	}
	
}
