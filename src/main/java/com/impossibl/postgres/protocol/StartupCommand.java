package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.MD5Authentication;

public class StartupCommand extends Command {
	
	Map<String, Object> params;
	boolean ready;
	
	public StartupCommand(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public void execute(final Context context) {
		
		ProtocolHandler handler = new AbstractProtocolHandler() {

			@Override
			public boolean isComplete() {
				return ready || error != null;
			}
			@Override
			public void backendKeyData(int processId, int secretKey) {
				context.setKeyData(processId, secretKey);
			}

			@Override
			public void authenticated(ProtocolV30 protocol) {
			}

			@Override
			public void authenticateKerberos(ProtocolV30 protocol) {
			}

			@Override
			public void authenticateClear(ProtocolV30 protocol) throws IOException {
				
				String password = context.getSetting("password").toString();

				protocol.sendPassword(password);
			}

			@Override
			public void authenticateCrypt(ProtocolV30 protocol) throws IOException {
			}

			@Override
			public void authenticateMD5(ProtocolV30 protocol, byte[] salt) throws IOException {
				
				String username = context.getSetting("username").toString();
				String password = context.getSetting("password").toString();

				String response = MD5Authentication.encode(password, username, salt);

				protocol.sendPassword(response);
			}

			@Override
			public void authenticateSCM(ProtocolV30 protocol) {
			}

			@Override
			public void authenticateGSS(ProtocolV30 protocol) {
			}

			@Override
			public void authenticateGSSCont(ProtocolV30 protocol) {
			}

			@Override
			public void authenticateSSPI(ProtocolV30 protocol) {
			}
			
		};
		
		try(ProtocolV30 protocol = context.lockProtocol(handler)) {
			
			protocol.sendStartup(params);

			protocol.run();
			
		}
		catch (IOException e) {
			error = new Error();
		}
		
	}

}
