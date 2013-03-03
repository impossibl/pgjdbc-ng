package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.UUID;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class UUIDs extends SimpleProcProvider {

	public UUIDs() {
		super(null, null, new Encoder(), new Decoder(), "uuid_");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public UUID decode(Type type, DataInputStream stream, Context context) throws IOException {
			long l = stream.readLong();
			long m = stream.readLong();
			return new UUID(m, l);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			UUID uval = (UUID)val;
			
			stream.writeLong(uval.getLeastSignificantBits());
			stream.writeLong(uval.getMostSignificantBits());
		}

	}

}
