package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Oid;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Oids extends SimpleProcProvider {

	public Oids() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "oid");
	}

	@Override
	protected boolean hasName(String name, String suffix, Context context) {
		if(context != null && name.equals(context.getSetting("blob.type", String.class)+suffix))
			return true;
		return super.hasName(name, suffix, context);
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Oid;
		}
		
		public Class<?> getOutputType() {
			return Integer.class;
		}

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

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Integer.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Oid;
		}
		
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

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Oid;
		}
		
		public Class<?> getOutputType() {
			return Integer.class;
		}

		public Integer decode(Type type, CharSequence buffer, Context context) throws IOException {
			
			return Integer.valueOf(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Integer.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Oid;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append((int)val);
		}

	}

}
