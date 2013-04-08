package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Float4s extends SimpleProcProvider {

	public Float4s() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "float4");
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Float;
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

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Float.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Float;
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

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Float;
		}
		
		public Class<?> getOutputType() {
			return Float.class;
		}

		public Float decode(Type type, CharSequence buffer, Context context) throws IOException {

			return Float.valueOf(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Float.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Float;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append((float)val);
		}

	}

}
