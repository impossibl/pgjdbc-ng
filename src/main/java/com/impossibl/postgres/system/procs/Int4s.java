package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Int4s extends SimpleProcProvider {

	public Int4s() {
		super(null, null, new Encoder(), new Decoder(), "int4", "oid", "tid", "xid", "cid", "regproc");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public Integer decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 4) {
				throw new IOException("invalid length");
			}
			
			return buffer.readInt();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(4);
				buffer.writeInt((Integer) val);
			}
			
		}

	}

}
