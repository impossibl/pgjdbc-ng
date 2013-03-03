package com.impossibl.postgres.system.procs;

import static java.nio.charset.StandardCharsets.UTF_8;

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
			
			byte[] buf = new byte[type.getLength()];
			stream.read(buf);
			return new String(buf, UTF_8);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {

			byte[] buf = val.toString().getBytes(UTF_8);
			stream.writeCString(val.toString());
		}

	}

}
