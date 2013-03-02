import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.postgresql.Driver;

import com.impossibl.postgres.BasicContext;
import com.impossibl.postgres.BasicStringCodec;
import com.impossibl.postgres.Postgres;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


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
		
		System.setProperty("org.postgresql.forcebinary", "true");
		
		Map<String,Class<?>> flagMap = new HashMap<String, Class<?>>();
		flagMap.put("<RAW>", null);
		
		new Driver();
		
		Connection conn = DriverManager.getConnection("jdbc:postgresql://db/impossibl", "postgres", "test");
		
		Postgres.init(conn, Version.get(9,2,0));
		
		PreparedStatement ps = conn.prepareStatement("select ('12345678',(null,'Tester'))::helper2");
		
		ResultSet rs = ps.executeQuery();
		rs.next();
		
		byte[][] row = (byte[][]) rs.getObject(-1, flagMap);
		
		ByteArrayOutputStream colByteStraem = new ByteArrayOutputStream(row[0].length+4);
		DataOutputStream colDataStream = new DataOutputStream(colByteStraem);
		colDataStream.writeInt(row[0].length);
		colDataStream.write(row[0]);
		
		Type type = Registry.loadType(16798);
		
		Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
		typeMap.put("helper2", Helper2.class);
		typeMap.put("role_granter", RoleGrant.class);
		
		BasicContext basicContext = new BasicContext(typeMap, new BasicStringCodec(UTF_8));
		Object val = type.getBinaryIO().send.handle(type, new DataInputStream(new ByteArrayInputStream(colByteStraem.toByteArray())), basicContext);
		
		rs.next();
		
		
	}
	
}
