package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Float8s extends SimpleProcProvider {

	public Float8s() {
		super(null, null, new Encoder(), new Decoder(), "float8");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public Double decode(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readDouble();
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeDouble((Double)val);
		}

	}

}
