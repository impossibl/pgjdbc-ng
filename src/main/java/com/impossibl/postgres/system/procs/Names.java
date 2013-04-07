package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.String;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


public class Names extends SimpleProcProvider {

	public Names() {
		super(null, null, new Encoder(), new Decoder(), "name");
	}
	
	static class Decoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return String;
		}
		
		public Class<?> getOutputType() {
			return String.class;
		}

		public String decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			
			byte[] bytes = new byte[length];
			buffer.readBytes(bytes);
			
			return new String(bytes, context.getCharset());
		}

	}

	static class Encoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return String.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return String;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			
			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
			
				byte[] bytes = val.toString().getBytes(context.getCharset());
			
				buffer.writeInt(bytes.length);
			
				buffer.writeBytes(bytes);
			}
			
		}

	}

}
