package com.impossibl.postgres.utils;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;

public class ChannelBuffers {
	
	public static String readCString(ChannelBuffer buffer, Charset charset) {
		
		byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
		buffer.readBytes(bytes);
				
		String res = new String(bytes, 0, bytes.length-1, charset);
		
		return res;
	}

	public static void writeCString(ChannelBuffer buffer, String val, Charset charset) {
		
		buffer.writeBytes(val.getBytes(charset));
		buffer.writeByte(0);
	}

}
