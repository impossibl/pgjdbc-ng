package com.impossibl.postgres.protocol;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class AuthenticationMP implements MessageProcessor {

	@Override
	public void process(Protocol proto, DataInputStream in, Context context) throws IOException {

		int code = in.readInt();
		switch(code) {
		case 0:
			
			//Ok
			
			context.authenticated();
			
			break;
		
		case 2:
			
			//KerberosV5
			
			break;
		
		case 3:
			
		//Cleartext
		{

			String response = context.getSetting("password").toString();
			
			proto.authenticate(response);
			
			return;
		}
		
		case 5:
			
		//MD5
		{
			byte[] saltData = new byte[4];
			in.readFully(saltData);
			String salt = new String(saltData, US_ASCII);
			
			String username = context.getSetting("username").toString();
			String password = context.getSetting("password").toString();
						
			String response =	concat("md5", md5(concat(md5(concat(password, username)), salt)));
			
			proto.authenticate(response);
			
			return;
		}
			
		case 6:
			
			//SCM Credential
			
			break;
			
		case 7:
			
			//GSS
			
			break;
			
		case 8:
			
			//GSS Continue
			
			break;
			
		case 9:
			
			//SSPI
			
			break;
			
		}

		throw new UnsupportedOperationException("invalid authentication type");
	}

	private String concat(String a, String b) {
		return a + b;
	}

	private String md5(String string) {

		return null;
	}

}
