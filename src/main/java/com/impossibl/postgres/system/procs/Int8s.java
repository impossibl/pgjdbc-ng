package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Int8s extends SimpleProcProvider {

	public Int8s() {
		super(null, null, new Receive(), new Send(), "int8");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Long handle(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readLong();
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeLong((Long)val);
		}

	}

}
