package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.MD5Authentication;

public class StartupProtocol extends Protocol {

	//Frontend messages
	private static final byte PASSWORD_MSG_ID 			= 'p';

	//Backend messages
	private static final byte BACKEND_KEY_MSG_ID 		= 'K';
	private static final byte AUTHENTICATION_MSG_ID = 'R';
	
	
	private boolean ready;

	
	public StartupProtocol(Context context) {
		super(context);
	}

	@Override
	public boolean isRunComplete() {
		return super.isRunComplete() || ready;
	}

	public void sendStartup(Map<String, Object> params) throws IOException {

		Message msg = new Message((byte)0);
		
		// Version
		msg.writeShort(3);
		msg.writeShort(0);

		// Name=Value pairs
		for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
			msg.writeCString(paramEntry.getKey());
			msg.writeCString(paramEntry.getValue().toString());
		}
		
		msg.writeByte(0);
		
		sendMessage(msg);
	}

	public void sendPassword(String password) throws IOException {
		
		Message msg = new Message(PASSWORD_MSG_ID);
		
		msg.writeCString(password);

		sendMessage(msg);
	}
	
	protected void authenticated() {
	}

	protected void authenticateKerberos() {
		//TODO: KerberosV5 authentication
		throw new UnsupportedOperationException();
	}

	protected void authenticateClear() throws IOException {
		
		sendPassword(context.getSetting("password").toString());
	}

	protected void authenticateCrypt() {
		//TODO: CRYPT authentication
		throw new UnsupportedOperationException();
	}

	protected void authenticateMD5(byte[] salt) throws IOException {

		String username = context.getSetting("username").toString();
		String password = context.getSetting("password").toString();

		String response = MD5Authentication.encode(password, username, salt);

		sendPassword(response);
	}

	protected void authenticateSCM() {
		//TODO: SCM authentication
		throw new UnsupportedOperationException();
	}

	protected void authenticateGSS() {
		//TODO: GSS authentication
		throw new UnsupportedOperationException();
	}

	protected void authenticateGSSCont() {
		throw new UnsupportedOperationException();
	}

	protected void authenticateSSPI() {
		//TODO: SSPI authentication
		throw new UnsupportedOperationException();
	}

	protected void backendKeyData(int processId, int secretKey) throws IOException {
		context.setKeyData(processId, secretKey);
	}
	
	@Override
	protected void readyForQuery(TransactionStatus txStatus) throws IOException {
		super.readyForQuery(txStatus);
		ready = true;
	}


	
	
	
	/*
	 * 
	 * Message dispatching & parsing
	 * 
	 */
	
	@Override
	public boolean dispatch(DataInputStream in, byte msgId) throws IOException {
		
		if(super.dispatch(in, msgId))
			return true;
		
		switch(msgId) {
		case AUTHENTICATION_MSG_ID:
			receiveAuthentication(in);
			return true;
			
		case BACKEND_KEY_MSG_ID:
			receiveBackendKeyData(in);
			return true;
		}
	
		return false;
	}
	
	
	private void receiveAuthentication(DataInputStream in) throws IOException {
		
		int code = in.readInt();
		switch (code) {
		case 0:

			// Ok
			authenticated();
			return;

		case 2:

			// KerberosV5
			authenticateKerberos();
			break;

		case 3:

			// Cleartext
			authenticateClear();
			return;

		case 4:

			// Crypt
			authenticateCrypt();
			return;

		case 5:

			// MD5
			byte[] salt = new byte[4];
			in.readFully(salt);

			authenticateMD5(salt);

			return;

		case 6:

			// SCM Credential
			authenticateSCM();
			break;

		case 7:

			// GSS
			authenticateGSS();
			break;

		case 8:

			// GSS Continue
			authenticateGSSCont();
			break;

		case 9:

			// SSPI
			authenticateSSPI();
			break;

		}

		throw new UnsupportedOperationException("invalid authentication type");
	}
	
	private void receiveBackendKeyData(DataInputStream in) throws IOException {
		
		int processId = in.readInt();
		int secretKey = in.readInt();
		
		backendKeyData(processId, secretKey);
	}

}
