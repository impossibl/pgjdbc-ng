package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

public abstract class TextEncoder implements Type.Codec.Encoder {

	abstract void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException;

	@Override
	public void encode(Type type, Object buffer, Object value, Context context) throws IOException {
		
		if(buffer instanceof ChannelBuffer) {
			
			ChannelBuffer channelBuffer = (ChannelBuffer) buffer;			
			
			StringBuilder tmp = new StringBuilder();

			encode(type, tmp, value, context);
			
			byte[] bytes = tmp.toString().getBytes(context.getCharset());
			
			channelBuffer.writeInt(bytes.length);
			channelBuffer.writeBytes(bytes);
		}
		else {
			
			encode(type, (StringBuilder)buffer, value, context);
		}
		
		
	}

}
