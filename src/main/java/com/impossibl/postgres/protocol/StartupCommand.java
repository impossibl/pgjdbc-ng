package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.Map;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.utils.MD5Authentication;

public class StartupCommand extends Command {
	
	Map<String, Object> params;
	boolean ready;
	
	public StartupCommand(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public void execute(final Context context) throws IOException {
		
		ProtocolHandler handler = new AbstractProtocolHandler() {

			@Override
			public boolean isComplete() {
				return ready || error != null;
			}
			
			@Override
			public void ready(TransactionStatus txStatus) {
				StartupCommand.this.ready = true;
			}

			@Override
			public void backendKeyData(int processId, int secretKey) {
				context.setKeyData(processId, secretKey);
			}

			@Override
			public void authenticated(Protocol protocol) {
			}

			@Override
			public void authenticateKerberos(Protocol protocol) {
			}

			@Override
			public void authenticateClear(Protocol protocol) throws IOException {
				
				String password = context.getSetting("password").toString();

				protocol.sendPassword(password);
			}

			@Override
			public void authenticateCrypt(Protocol protocol) throws IOException {
			}

			@Override
			public void authenticateMD5(Protocol protocol, byte[] salt) throws IOException {
				
				String username = context.getSetting("username").toString();
				String password = context.getSetting("password").toString();

				String response = MD5Authentication.encode(password, username, salt);

				protocol.sendPassword(response);
			}

			@Override
			public void authenticateSCM(Protocol protocol) {
			}

			@Override
			public void authenticateGSS(Protocol protocol) {
			}

			@Override
			public void authenticateGSSCont(Protocol protocol) {
			}

			@Override
			public void authenticateSSPI(Protocol protocol) {
			}
			
		};
		
		try(Protocol protocol = context.lockProtocol(handler)) {
			
			protocol.sendStartup(params);

			protocol.run();
			
		}
		
	}

}
