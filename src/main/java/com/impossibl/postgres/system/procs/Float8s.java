package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Double;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Float8s extends SimpleProcProvider {

	public Float8s() {
		super(null, null, new Encoder(), new Decoder(), "float8");
	}

	static class Decoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Double;
		}
		
		public Class<?> getOutputType() {
			return Double.class;
		}

		public Double decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}
			
			return buffer.readDouble();
		}

	}

	static class Encoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Double.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Double;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(8);
				buffer.writeDouble((Double) val);
			}
			
		}

	}

}
