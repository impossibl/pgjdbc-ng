package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Int8;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


public class Int8s extends SimpleProcProvider {

	public Int8s() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "int8");
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Int8;
		}
		
		public Class<?> getOutputType() {
			return Long.class;
		}

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

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Long.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Int8;
		}
		
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

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Int8;
		}
		
		public Class<?> getOutputType() {
			return Long.class;
		}

		public Long decode(Type type, CharSequence buffer, Context context) throws IOException {
			
			return Long.valueOf(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Long.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Int8;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append((long)val);
		}

	}

}
