package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;


public class Procs {
		
	
	public static final Type.Codec.Decoder[] defaultDecoders = { Strings.BINARY_DECODER, Bytes.BINARY_DECODER };
	
	private static ProcProvider[] PROVIDERS = {
		new Oids(),
		new Bools(),
		new Bits(),
		new Bytes(),
		new Float4s(),
		new Float8s(),
		new Int2s(),
		new Int4s(),
		new Int8s(),
		new Numerics(),
		new NumericMods(),
		new Names(),
		new Strings(),
		new Dates(),
		new TimesWithoutTZ(),
		new TimesWithTZ(),
		new TimeMods(),
		new TimestampsWithoutTZ(),
		new TimestampsWithTZ(),
		new TimestampMods(),
		new Intervals(),
		new UUIDs(),
		new XMLs(),
		new Moneys(),
		new Arrays(),
		new Records(),
		new ACLItems(),
	};
	
	private static final Type.Codec.Decoder[] DEFAULT_DECODERS = { new Unknowns.TxtDecoder(), new Unknowns.BinDecoder() };
	private static final Type.Codec.Encoder[] DEFAULT_ENCODERS = { new Unknowns.TxtEncoder(), new Unknowns.BinEncoder() };
	private static final Modifiers.Parser DEFAULT_MOD_PARSER = new Unknowns.ModParser();
	
	public static Type.Codec.Decoder getDefaultDecoder(Format format) {
		return DEFAULT_DECODERS[format.ordinal()];
	}
	
	public static Type.Codec.Encoder getDefaultEncoder(Format format) {
		return DEFAULT_ENCODERS[format.ordinal()];
	}
	
	public static Modifiers.Parser getDefaultModParser() {
		return DEFAULT_MOD_PARSER;
	}
	

	public static Codec loadNamedTextCodec(String baseName, Context context) {
		Codec codec = new Codec();
		codec.encoder = loadEncoderProc(baseName + "in", 	context, DEFAULT_ENCODERS[Format.Text.ordinal()]);
		codec.decoder = loadDecoderProc(baseName + "out", context, DEFAULT_DECODERS[Format.Text.ordinal()]);
		return codec;
	}

	public static Codec loadNamedBinaryCodec(String baseName, Context context) {
		Codec codec = new Codec();
		codec.encoder = loadEncoderProc(baseName + "recv", context, DEFAULT_ENCODERS[Format.Binary.ordinal()]);
		codec.decoder = loadDecoderProc(baseName + "send", context, DEFAULT_DECODERS[Format.Binary.ordinal()]);
		return codec;
	}

	public static Codec.Encoder loadEncoderProc(String name, Context context, Type.Codec.Encoder defaultEncoder) {
		
		if(!name.isEmpty()) {
			Codec.Encoder h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findEncoder(name, context)) != null)
					return h;
			}
		}

		return defaultEncoder;
	}

	public static Codec.Decoder loadDecoderProc(String name, Context context, Type.Codec.Decoder defaultDecoder) {
		
		if(!name.isEmpty()) {
			Codec.Decoder h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findDecoder(name, context)) != null)
					return h;
			}
		}

		return defaultDecoder;
	}

	public static Modifiers.Parser loadModifierParserProc(String name, Context context) {

		if(!name.isEmpty()) {
			Modifiers.Parser p;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((p = pp.findModifierParser(name, context)) != null)
					return p;
			}
		}

		return DEFAULT_MOD_PARSER;
	}

}
