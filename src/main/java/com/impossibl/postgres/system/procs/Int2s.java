package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Int2;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Int2s extends SimpleProcProvider {

	public Int2s() {
		super(null, null, new Encoder(), new Decoder(), "int2");
	}

	static class Decoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Int2;
		}
		
		public Class<?> getOutputType() {
			return Short.class;
		}

		public Short decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 2) {
				throw new IOException("invalid length");
			}
			
			return buffer.readShort();
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Short.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Int2;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(2);
				buffer.writeByte((Short) val);
			}
			
		}

	}

}
