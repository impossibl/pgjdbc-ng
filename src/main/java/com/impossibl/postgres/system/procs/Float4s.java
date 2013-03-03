package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Float4s extends SimpleProcProvider {

	public Float4s() {
		super(null, null, new Encoder(), new Decoder(), "float4");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public Float decode(Type type, DataInputStream stream, Context context) throws IOException {			
			if(stream.readInt() != 4) throw new IOException("invalid length");
			return stream.readFloat();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeFloat((Float)val);
		}

	}

}
