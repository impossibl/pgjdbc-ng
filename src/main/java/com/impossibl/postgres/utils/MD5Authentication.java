package com.impossibl.postgres.utils;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

public class MD5Authentication {

	/**
	 * Generates an authentication token compatible with PostgreSQL's
	 * MD5 scheme
	 * 
	 * @param password Password to generate token from
	 * @param user Username to generate token from
	 * @param salt Salt to generate token from
	 * @return MD5 authentication token
	 */
	public static String encode(String password, String user, byte salt[]) {
		
		HexBinaryAdapter hex = new HexBinaryAdapter();
		byte[] tempDigest, passDigest;

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

		byte[] hexDigest = hex.marshal(tempDigest).toLowerCase().getBytes(US_ASCII);
		
		md.update(hexDigest);
		md.update(salt);
		passDigest = md.digest();
		
		return "md5" + hex.marshal(passDigest).toLowerCase();
	}

}
