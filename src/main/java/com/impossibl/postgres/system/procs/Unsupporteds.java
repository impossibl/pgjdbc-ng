package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;


/*
 * Unsupported codec
 */
public class Unsupporteds implements ProcProvider {

	static class BinDecoder implements Type.BinaryIO.Decoder {

		String name;
		BinDecoder(String name) {
			this.name = name;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching binary decoder found for: " + name);
		}

	}

	static class BinEncoder implements Type.BinaryIO.Encoder {

		String name;
		BinEncoder(String name) {
			this.name = name;
		}

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching binary encoder found for: " + name);
		}

	}

	static class TxtDecoder implements Type.TextIO.Decoder {

		String name;
		TxtDecoder(String name) {
			this.name = name;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching text decoder found for: " + name);
		}

	}

	static class TxtEncoder implements Type.TextIO.Encoder {
		
		String name;
		TxtEncoder(String name) {
			this.name = name;
		}

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching text encoder found for: " + name);
		}

	}

	@Override
	public BinaryIO.Encoder findBinaryEncoder(String name, Context context) {
		return new BinEncoder(name);
	}

	@Override
	public BinaryIO.Decoder findBinaryDecoder(String name, Context context) {
		return new BinDecoder(name);
	}

	@Override
	public TextIO.Encoder findTextEncoder(String name, Context context) {
		return new TxtEncoder(name);
	}

	@Override
	public TextIO.Decoder findTextDecoder(String name, Context context) {
		return new TxtDecoder(name);
	}
	
}
