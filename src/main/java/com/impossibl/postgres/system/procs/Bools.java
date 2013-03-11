package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Bools extends SimpleProcProvider {

	public Bools() {
		super(null, null, new Encoder(), new Decoder(), "bool");
	}

	static class Decoder implements Type.Codec.Decoder {

		public Boolean decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 1) {
				throw new IOException("invalid length");
			}
			
			return buffer.readByte() != 0;
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			
			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(1);
				buffer.writeByte(((Boolean) val) ? 1 : 0);
			}
			
		}

	}

}
