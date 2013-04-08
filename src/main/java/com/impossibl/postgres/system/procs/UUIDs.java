package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


public class UUIDs extends SimpleProcProvider {

	public UUIDs() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "uuid_");
	}
	
	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.UUID;
		}
		
		public Class<?> getOutputType() {
			return UUID.class;
		}

		public UUID decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if(length != 16) {
				throw new IOException("invalid length");
			}
						
			return new UUID(buffer.readLong(), buffer.readLong());
		}

	}

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return UUID.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.UUID;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			
			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(16);
				
				UUID uval = (UUID)val;			
				buffer.writeLong(uval.getMostSignificantBits());
				buffer.writeLong(uval.getLeastSignificantBits());
			}
			
		}

	}

	static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.UUID;
		}
		
		public Class<?> getOutputType() {
			return UUID.class;
		}

		public UUID decode(Type type, CharSequence buffer, Context context) throws IOException {

			return UUID.fromString(buffer.toString());
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return UUID.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.UUID;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
			buffer.append(val.toString());
		}

	}

}
