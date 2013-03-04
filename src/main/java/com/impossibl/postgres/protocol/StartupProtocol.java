package com.impossibl.postgres.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class StartupProtocol extends Protocol {

	//Frontend messages
	private static final byte PASSWORD_MSG_ID 			= 'p';

	//Backend messages
	private static final byte BACKEND_KEY_MSG_ID = 'K';
	private static final byte AUTHENTICATION_MSG_ID = 'R';

	
	public StartupProtocol(Context context) {
		super(context);
	}

	public void startup(Map<String, Object> params) throws IOException {

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

	public void password(String password) throws IOException {
		
		Message msg = new Message(PASSWORD_MSG_ID);
		
		msg.writeCString(password);

		sendMessage(msg);
	}
	
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
	
	
	protected void authentication() throws IOException {
		
	}
	
	private void receiveAuthentication(DataInputStream in) throws IOException {
		
		int code = in.readInt();
		switch (code) {
		case 0:

			// Ok
			return;

		case 2:

			// KerberosV5
			break;

		case 3:

			// Cleartext
			password(context.getSetting("password").toString());
			return;

		case 4:

			// Crypt
			return;

		case 5:

			// MD5
			byte[] salt = new byte[4];
			in.readFully(salt);

			String username = context.getSetting("username").toString();
			String password = context.getSetting("password").toString();

			String response = md5(password, username, salt);

			password(response);

			return;

		case 6:

			// SCM Credential
			break;

		case 7:

			// GSS
			break;

		case 8:

			// GSS Continue
			break;

		case 9:

			// SSPI
			break;

		}

		throw new UnsupportedOperationException("invalid authentication type");
	}
	
	String md5(String password, String user, byte salt[]) {
		
		byte[] tempDigest, passDigest;
		byte[] hexDigest = new byte[35];

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		md.update(password.getBytes(UTF_8));
		md.update(user.getBytes(UTF_8));
		tempDigest = md.digest();

		bytesToHex(tempDigest, hexDigest, 0);
		md.update(hexDigest, 0, 32);
		md.update(salt);
		passDigest = md.digest();

		bytesToHex(passDigest, hexDigest, 3);
		hexDigest[0] = (byte) 'm';
		hexDigest[1] = (byte) 'd';
		hexDigest[2] = (byte) '5';

		return new String(hexDigest, UTF_8);
	}

	void bytesToHex(byte[] bytes, byte[] hex, int offset) {
		final char lookup[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		int i, c, j, pos = offset;

		for (i = 0; i < 16; i++) {
			c = bytes[i] & 0xFF;
			j = c >> 4;
			hex[pos++] = (byte) lookup[j];
			j = (c & 0xF);
			hex[pos++] = (byte) lookup[j];
		}
	}

	
	protected void backendKeyData(int processId, int secretKey) throws IOException {
		context.setKeyData(processId, secretKey);
	}
	
	private void receiveBackendKeyData(DataInputStream in) throws IOException {
		
		int processId = in.readInt();
		int secretKey = in.readInt();
		
		backendKeyData(processId, secretKey);
	}

}
