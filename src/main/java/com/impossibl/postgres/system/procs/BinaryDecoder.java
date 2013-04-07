package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

public abstract class BinaryDecoder implements Type.Codec.Decoder {

	abstract Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException;

	@Override
	public Object decode(Type type, Object buffer, Context context) throws IOException {
		return decode(type, (ChannelBuffer)buffer, context);
	}

}
