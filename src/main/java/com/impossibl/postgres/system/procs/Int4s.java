package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Int4s extends SimpleProcProvider {

	public Int4s() {
		super(null, null, new Receive(), new Send(), "int4", "oid", "tid", "xid", "cid");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Integer handle(Type type, DataInputStream stream, Context context) throws IOException {			
			return stream.readInt();
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeInt((Integer)val);
		}

	}

}
