package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Chars extends SimpleProcProvider {

	public Chars() {
		super(null, null, new Encoder(), new Decoder(), "char");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public Character decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 1) {
				throw new IOException("invalid length");
			}
			
			return (char) buffer.readByte();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(1);
				buffer.writeByte((byte) ((Character) val).charValue());
			}
			
		}

	}

}
