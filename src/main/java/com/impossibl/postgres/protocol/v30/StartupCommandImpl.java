package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;

import java.io.IOException;
import java.util.Map;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.utils.MD5Authentication;



public class StartupCommandImpl extends CommandImpl implements StartupCommand {

	Map<String, Object> params;
	boolean ready;

	public StartupCommandImpl(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public void execute(final ProtocolImpl protocol) throws IOException {

		ProtocolListener listener = new BaseProtocolListener() {

			@Override
			public boolean isComplete() {
				return ready || error != null;
			}

			@Override
			public synchronized void ready(TransactionStatus txStatus) {
				StartupCommandImpl.this.ready = true;
				notify();
			}

			@Override
			public synchronized void error(Error error) {
				setError(error);
			}

			@Override
			public void backendKeyData(int processId, int secretKey) {
				protocol.context.setKeyData(processId, secretKey);
			}

			@Override
			public void authenticated(ProtocolImpl protocol) {
			}

			@Override
			public void authenticateKerberos(ProtocolImpl protocol) {
			}

			@Override
			public void authenticateClear(ProtocolImpl protocol) throws IOException {

				String password = protocol.context.getSetting(CREDENTIALS_PASSWORD).toString();

				protocol.sendPassword(password);
			}

			@Override
			public void authenticateCrypt(ProtocolImpl protocol) throws IOException {
			}

			@Override
			public void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException {

				String username = protocol.context.getSetting(CREDENTIALS_USERNAME).toString();
				String password = protocol.context.getSetting(CREDENTIALS_PASSWORD).toString();

				String response = MD5Authentication.encode(password, username, salt);

				protocol.sendPassword(response);
			}

			@Override
			public void authenticateSCM(ProtocolImpl protocol) {
			}

			@Override
			public void authenticateGSS(ProtocolImpl protocol) {
			}

			@Override
			public void authenticateGSSCont(ProtocolImpl protocol) {
			}

			@Override
			public void authenticateSSPI(ProtocolImpl protocol) {
			}

		};

		protocol.setListener(listener);

		protocol.sendStartup(params);

		waitFor(listener);
	}

}
