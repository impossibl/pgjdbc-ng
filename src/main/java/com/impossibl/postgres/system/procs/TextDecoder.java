package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

public abstract class TextDecoder implements Type.Codec.Decoder {

	abstract Object decode(Type type, CharSequence buffer, Context context) throws IOException;

	@Override
	public Object decode(Type type, Object buffer, Context context) throws IOException {
		
		if(buffer instanceof ChannelBuffer)  {
			
			CharSequence textBuffer = (CharSequence)Strings.BINARY_DECODER.decode(type, buffer, context);
			if(textBuffer == null)
				return null;
			
			return decode(type, textBuffer, context);
		}
		
		return decode(type, (CharSequence)buffer, context);
	}

}
