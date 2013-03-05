package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


/*
 * Unsupported codec
 */
public class Unsupporteds implements ProcProvider {

	static class BinDecoder implements Type.BinaryIO.Decoder {

		String name;
		BinDecoder(String name) {
			this.name = name;
		}

		public Object decode(Type type, DataInputStream stream, Context context) throws IOException {
			throw new UnssupportedFormatException();
		}

	}

	static class BinEncoder implements Type.BinaryIO.Encoder {

		String name;
		BinEncoder(String name) {
			this.name = name;
		}

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException();
		}

	}

	static class TxtDecoder implements Type.TextIO.Decoder {

		String name;
		TxtDecoder(String name) {
			this.name = name;
		}

		public Object decode(Type type, Reader reader, Context context) throws IOException {
			throw new UnssupportedFormatException();
		}

	}

	static class TxtEncoder implements Type.TextIO.Encoder {
		
		String name;
		TxtEncoder(String name) {
			this.name = name;
		}

		public void encode(Type type, Writer writer, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException();
		}

	}

	@Override
	public BinaryIO.Encoder findBinaryEncoder(String name) {
		return new BinEncoder(name);
	}

	@Override
	public BinaryIO.Decoder findBinaryDecoder(String name) {
		return new BinDecoder(name);
	}

	@Override
	public TextIO.Encoder findTextEncoder(String name) {
		return new TxtEncoder(name);
	}

	@Override
	public TextIO.Decoder findTextDecoder(String name) {
		return new TxtDecoder(name);
	}
	
}
