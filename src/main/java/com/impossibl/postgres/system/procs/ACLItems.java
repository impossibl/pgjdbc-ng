package com.impossibl.postgres.system.procs;

import java.io.IOException;

import com.impossibl.postgres.data.ACLItem;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class ACLItems extends SimpleProcProvider {

	public ACLItems() {
		super(new Encoder(), new Decoder(), null, null, "aclitem");
	}

	static class Decoder extends TextDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.ACLItem;
		}

		public Class<?> getOutputType() {
			return ACLItem.class;
		}

		public ACLItem decode(Type type, CharSequence buffer, Context context) throws IOException {

			return ACLItem.parse(buffer.toString());
		}
	}

	static class Encoder extends TextEncoder {

		public Class<?> getInputType() {
			return ACLItem.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.ACLItem;
		}

		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

		}

	}

}
