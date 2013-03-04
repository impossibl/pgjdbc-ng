package com.impossibl.postgres.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Authentication {

	public static String encode(String password, String user, byte salt[]) {
		
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

	static void bytesToHex(byte[] bytes, byte[] hex, int offset) {
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
