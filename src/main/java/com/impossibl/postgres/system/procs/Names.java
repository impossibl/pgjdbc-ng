package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.String;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


public class Names extends SimpleProcProvider {

	public Names() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "name");
	}
	
	static class BinDecoder extends BinaryDecoder {

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

	static class BinEncoder extends BinaryEncoder {

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

	public static class TxtDecoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return String;
		}
		
		public Class<?> getOutputType() {
			return String.class;
		}

		public String decode(Type type, CharSequence buffer, Context context) throws IOException {
			
			return buffer.toString();
		}

	}

	public static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return String.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return String;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

			buffer.append((String)val);
		}

	}

}
