package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;


/*
 * Unsupported codec
 */
public class Unsupporteds implements ProcProvider {

	static class Decoder implements Type.Codec.Decoder {

		String name;
		
		Decoder(String name) {
			this.name = name;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching decoder found for: " + name);
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		String name;
		
		Encoder(String name) {
			this.name = name;
		}

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching encoder found for: " + name);
		}

	}

	@Override
	public Codec.Encoder findEncoder(String name, Context context) {
		return new Encoder(name);
	}

	@Override
	public Codec.Decoder findDecoder(String name, Context context) {
		return new Decoder(name);
	}

}
