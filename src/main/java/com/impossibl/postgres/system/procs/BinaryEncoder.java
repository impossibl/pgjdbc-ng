package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

public abstract class BinaryEncoder implements Type.Codec.Encoder {

	abstract void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException;

	@Override
	public void encode(Type type, Object buffer, Object value, Context context) throws IOException {
		encode(type, (ChannelBuffer)buffer, value, context);
	}

}
