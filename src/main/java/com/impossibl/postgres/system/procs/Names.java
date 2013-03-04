package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Names extends SimpleProcProvider {

	public Names() {
		super(null, null, new Encoder(), new Decoder(), "name");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public String decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if(length == -1) {
				return null;
			}
			
			byte[] bytes = new byte[length];
			stream.readFully(bytes);
			
			return context.getStringCodec().decode(bytes);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			if (val == null) {
				
				stream.writeInt(-1);
			}
			else {
			
				byte[] bytes = context.getStringCodec().encode(val.toString());
			
				stream.writeInt(bytes.length);
			
				stream.write(bytes);
			}
			
		}

	}

}
