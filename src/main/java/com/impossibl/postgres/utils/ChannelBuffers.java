package com.impossibl.postgres.utils;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.jboss.netty.buffer.ChannelBuffer;

public class ChannelBuffers {
	
	public static String readCString(ChannelBuffer buffer) {
		
		byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
		buffer.readBytes(bytes);
				
		String res = new String(bytes, 0, bytes.length-1, US_ASCII);
		
		return res;
	}

	public static String readCStringUtf8(ChannelBuffer buffer) {
		byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
		buffer.readBytes(bytes);

		String res = new String(bytes, 0, bytes.length-1, UTF_8);

		return res;
	}

	public static void writeCString(ChannelBuffer buffer, String val) {
		
		buffer.writeBytes(val.getBytes(US_ASCII));
		buffer.writeByte(0);
	}

	public static void writeCStringUtf8(ChannelBuffer buffer, String val) {
		buffer.writeBytes(val.getBytes(UTF_8));
		buffer.writeByte(0);
	}

}
