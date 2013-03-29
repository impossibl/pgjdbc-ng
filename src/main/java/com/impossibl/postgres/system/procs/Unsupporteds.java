package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.PrimitiveType;
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

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Unknown;
		}
		
		public Class<Void> getOutputType() {
			return Void.class;
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

		public Class<Void> getInputType() {
			return Void.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Unknown;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			throw new UnssupportedFormatException("No matching encoder found for: " + name);
		}

	}
	
	static class ModParser implements Modifiers.Parser {

		@Override
		public Map<String, Object> parse(long mod) {
			return Collections.<String,Object>emptyMap();
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

	@Override
	public Modifiers.Parser findModifierParser(String name, Context context) {
		return new ModParser();
	}
	

}
