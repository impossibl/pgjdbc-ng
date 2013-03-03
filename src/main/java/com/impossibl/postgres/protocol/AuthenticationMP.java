package com.impossibl.postgres.protocol;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;



public class AuthenticationMP implements MessageProcessor {

	HexBinaryAdapter hex = new HexBinaryAdapter();

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {

		Context context = handler.getContext();
		Protocol proto = handler.getContext().getProtocol();

		int code = in.readInt();
		switch (code) {
		case 0:

			// Ok

			context.authenticated();

			return;

		case 2:

			// KerberosV5

			break;

		case 3:

		// Cleartext
		{

			String response = context.getSetting("password").toString();

			proto.authenticate(response);

			return;
		}

		case 4:

			// Crypt

			return;

		case 5:

		// MD5
		{
			byte[] salt = new byte[4];
			in.readFully(salt);

			String username = context.getSetting("username").toString();
			String password = context.getSetting("password").toString();

			String response = md5(password, username, salt);

			proto.authenticate(response);

			return;
		}

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

		return new String(hexDigest, US_ASCII);
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

}
