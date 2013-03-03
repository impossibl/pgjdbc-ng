package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Bytes extends SimpleProcProvider {

	public Bytes() {
		super(null, null, new Encoder(), new Decoder(), "bytea");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public Byte decode(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readByte();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeByte((Byte)val);
		}

	}

}
