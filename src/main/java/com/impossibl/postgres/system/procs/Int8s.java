package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;


public class Int8s extends SimpleProcProvider {

	public Int8s() {
		super(null, null, new Encoder(), new Decoder(), "int8");
	}

	static class Decoder implements Type.Codec.Decoder {

		public Long decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			return buffer.readLong();
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(8);
				buffer.writeLong((Long) val);
			}
			
		}

	}

}
