package com.impossibl.postgres.utils;

import static java.nio.charset.StandardCharsets.US_ASCII;

import org.jboss.netty.buffer.ChannelBuffer;

public class ChannelBuffers {
	
	public static String readCString(ChannelBuffer buffer) {
		
		byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
		buffer.readBytes(bytes);
				
		String res = new String(bytes, 0, bytes.length-1, US_ASCII);
		
		return res;
	}

	public static void writeCString(ChannelBuffer buffer, String val) {
		
		buffer.writeBytes(val.getBytes(US_ASCII));
		buffer.writeByte(0);
	}

}
