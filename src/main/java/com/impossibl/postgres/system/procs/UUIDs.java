package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.UUID;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class UUIDs extends SimpleProcProvider {

	public UUIDs() {
		super(null, null, new Receive(), new Send(), "uuid_");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public UUID handle(Type type, DataInputStream stream, Context context) throws IOException {
			long l = stream.readLong();
			long m = stream.readLong();
			return new UUID(m, l);
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			UUID uval = (UUID)val;
			
			stream.writeLong(uval.getLeastSignificantBits());
			stream.writeLong(uval.getMostSignificantBits());
		}

	}

}
