package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Bools extends SimpleProcProvider {

	public Bools() {
		super(null, null, new Receive(), new Send(), "bool");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Boolean handle(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readByte() != 0;
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeBoolean((Boolean)val);
		}

	}

}
