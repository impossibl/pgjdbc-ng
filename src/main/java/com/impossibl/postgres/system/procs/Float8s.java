package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Float8s extends SimpleProcProvider {

	public Float8s() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "float8");
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Double;
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

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Double.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Double;
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

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Double;
		}
		
		public Class<?> getOutputType() {
			return Double.class;
		}

		public Double decode(Type type, CharSequence buffer, Context context) throws IOException {

			return Double.valueOf(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Double.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Double;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append((double)val);
		}

	}

}
