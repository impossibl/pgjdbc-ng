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

  private final static byte[] hex = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
  public static void writeLongAsCString(ChannelBuffer buffer, long val) {
		if (val == 0) {
			buffer.writeByte(0);
			return;
		}

		byte[] buf = new byte[16 + 1];
		int pos = 16;
		while (val != 0) {
			buf[--pos] = hex[(int) (val & 15)];
			val >>>= 4;
	  }

		buffer.writeBytes(buf, pos, 17-pos);
	}
}
