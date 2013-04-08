package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Bool;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Bools extends SimpleProcProvider {

	public Bools() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "bool");
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Bool;
		}
		
		public Class<?> getOutputType() {
			return Boolean.class;
		}

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

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Boolean.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Bool;
		}
		
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

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Bool;
		}
		
		public Class<?> getOutputType() {
			return Boolean.class;
		}

		public Boolean decode(Type type, CharSequence buffer, Context context) throws IOException {

			return Boolean.valueOf(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Boolean.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Bool;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append((boolean)val);
		}

	}

}
