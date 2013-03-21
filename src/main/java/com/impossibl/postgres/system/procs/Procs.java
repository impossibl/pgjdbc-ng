package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Timestamp;
import static com.impossibl.postgres.types.PrimitiveType.TimestampTZ;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type.Codec;


public class Procs {
	
	private static ProcProvider[] PROVIDERS = {
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
		new Timestamps(Timestamp, "timestamp_"),
		new Timestamps(TimestampTZ, "timestamptz_"),
		new UUIDs(),
		new Moneys(),
		new Arrays(),
		new Records(),
	};
	
	private static final Unsupporteds UNSUPPORTEDS = new Unsupporteds(); 

	public static Codec loadNamedTextCodec(String baseName, Context context) {
		Codec codec = new Codec();
		codec.encoder = loadEncoderProc(baseName + "in", context);
		codec.decoder = loadDecoderProc(baseName + "out", context);
		return codec;
	}

	public static Codec loadNamedBinaryCodec(String baseName, Context context) {
		Codec codec = new Codec();
		codec.encoder = loadEncoderProc(baseName + "recv", context);
		codec.decoder = loadDecoderProc(baseName + "send", context);
		return codec;
	}

	public static Codec.Encoder loadEncoderProc(String name, Context context) {
		
		if(!name.isEmpty()) {
			Codec.Encoder h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findEncoder(name, context)) != null)
					return h;
			}
		}

		return UNSUPPORTEDS.findEncoder(name, context);
	}

	public static Codec.Decoder loadDecoderProc(String name, Context context) {
		
		if(!name.isEmpty()) {
			Codec.Decoder h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findDecoder(name, context)) != null)
					return h;
			}
		}

		return UNSUPPORTEDS.findDecoder(name, context);
	}

	public static Modifiers.Parser loadModifierParserProc(String name, Context context) {

		if(!name.isEmpty()) {
			Modifiers.Parser p;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((p = pp.findModifierParser(name, context)) != null)
					return p;
			}
		}

		return UNSUPPORTEDS.findModifierParser(name, context);
	}

}
