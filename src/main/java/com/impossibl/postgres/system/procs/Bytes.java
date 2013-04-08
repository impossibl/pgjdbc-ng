package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.types.PrimitiveType.Binary;
import static java.lang.Math.min;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Bytes extends SimpleProcProvider {
	
	public static final BinDecoder BINARY_DECODER = new BinDecoder();
	public static final BinEncoder BINARY_ENCODER = new BinEncoder();

	public Bytes() {
		super(null, null, new BinEncoder(), new BinDecoder(), "bytea");
	}

	static class BinDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Binary;
		}
		
		public Class<?> getOutputType() {
			return byte[].class;
		}

		public byte[] decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}

			byte[] bytes;
			
			Integer maxLength = (Integer) context.getSetting(FIELD_VARYING_LENGTH_MAX);
			if(maxLength != null) {
				bytes = new byte[min(maxLength, length)];
			}
			else {
				bytes = new byte[length];				
			}
			
			buffer.readBytes(bytes);
			buffer.skipBytes(length - bytes.length);
			
			return bytes;
		}

	}

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return byte[].class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Binary;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				byte[] bytes = (byte[]) val;
				
				buffer.writeInt(bytes.length);
				buffer.writeBytes(bytes);
			}
			
		}

	}

}
