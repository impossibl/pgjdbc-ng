package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static java.lang.Math.min;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Bytes extends SimpleProcProvider {

	public Bytes() {
		super(null, null, new Encoder(), new Decoder(), "bytea");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

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

	static class Encoder implements Type.BinaryIO.Encoder {

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
