package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Bits;

import java.io.IOException;
import java.util.BitSet;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Bits extends SimpleProcProvider {

	public Bits() {
		super(null, null, new Encoder(), new Decoder(), "bit_", "varbit_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Bits;
		}
		
		public Class<?> getOutputType() {
			return BitSet.class;
		}

		public BitSet decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}

			int bitCount = buffer.readInt();
			int byteCount = (bitCount + 7) / 8;

			byte[] bytes = new byte[byteCount];
			buffer.readBytes(bytes);

			// Set equivalent bits in bit set (they use reversed encodings so
			// they cannot be just copied in
			BitSet bs = new BitSet(bitCount);
			for (int c = 0; c < bitCount; ++c) {
				bs.set(c, (bytes[c / 8] & (0x80 >> (c % 8))) != 0);
			}

			return bs;
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return BitSet.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Bits;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {

				buffer.writeInt(-1);
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

				buffer.writeInt(bs.size());
				buffer.writeBytes(bytes);
			}

		}

	}

}
