package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Strings extends SimpleProcProvider {

	public Strings() {
		super(null, null, new Receive(), new Send(), "text", "varchar", "bpchar", "enum_", "json_", "xml_");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public String handle(Type type, DataInputStream stream, Context context) throws IOException {
			
			int len = stream.readInt();
			byte[] bytes = new byte[len];
			stream.readFully(bytes);
			
			return context.getStringCodec().decode(bytes);
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			byte[] bytes = context.getStringCodec().encode(val.toString());
			
			stream.writeInt(bytes.length);
			stream.write(bytes);
		}

	}

}
