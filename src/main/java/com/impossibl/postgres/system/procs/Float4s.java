package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.sql.Types;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Float4s extends SimpleProcProvider {

	public Float4s() {
		super(null, null, new Encoder(), new Decoder(), "float4");
	}

	static class Decoder implements Type.Codec.Decoder {

		public int getInputSQLType() {
			return Types.REAL;
		}
		
		public Class<?> getOutputType() {
			return Float.class;
		}

		public Float decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 4) {
				throw new IOException("invalid length");
			}
			
			return buffer.readFloat();
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Float.class;
		}

		public int getOutputSQLType() {
			return Types.REAL;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(4);
				buffer.writeFloat((Float) val);
			}
			
		}

	}

}
