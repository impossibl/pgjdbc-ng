package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.BitSet;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Bits extends SimpleProcProvider {

	public Bits() {
		super(null, null, new Encoder(), new Decoder(), "bit_", "varbit_");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public BitSet decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if (length == -1) {
				return null;
			}

			int bitCount = stream.readInt();
			int byteCount = (bitCount + 7) / 8;

			byte[] bytes = new byte[byteCount];
			stream.readFully(bytes);

			// Set equivalent bits in bit set (they use reversed encodings so
			// they cannot be just copied in
			BitSet bs = new BitSet(bitCount);
			for (int c = 0; c < bitCount; ++c) {
				bs.set(c, (bytes[c / 8] & (0x80 >> (c % 8))) != 0);
			}

			return bs;
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {

			if (val == null) {

				stream.writeInt(-1);
			}
			else {

				BitSet bs = (BitSet) val;

				int bitCount = bs.size();
				int byteCount = (bitCount + 7) / 8;

				// Set equivalent bits in byte array (they use reversed encodings so
				// they cannot be just copied in
				byte[] bytes = new byte[byteCount];
				for (int c = 0; c < bitCount; ++c) {
					bytes[c / 8] |= ((0x80 >> (c % 8)) & (bs.get(c) ? 0xff : 0x00));
				}

				stream.writeInt(bs.size());
				stream.write(bytes);
			}

		}

	}

}
