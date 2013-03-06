package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Int2s extends SimpleProcProvider {

	public Int2s() {
		super(null, null, new Encoder(), new Decoder(), "int2");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public Short decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 2) {
				throw new IOException("invalid length");
			}
			
			return stream.readShort();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {

			if (val == null) {
				
				stream.writeInt(-1);
			}
			else {
				
				stream.writeInt(2);
				stream.writeByte((Short) val);
			}
			
		}

	}

}
