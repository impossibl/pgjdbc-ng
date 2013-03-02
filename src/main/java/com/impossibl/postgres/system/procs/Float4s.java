package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Float4s extends SimpleProcProvider {

	public Float4s() {
		super(null, null, new Receive(), new Send(), "float4");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Float handle(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readFloat();
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeFloat((Float)val);
		}

	}

}
