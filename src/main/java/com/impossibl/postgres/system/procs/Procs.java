package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;


public class Procs {
	
	private static ProcProvider[] PROVIDERS = {
		new Bools(),
		new Bits(),
		new Bytes(),
		new Chars(),
		new Float4s(),
		new Float8s(),
		new Int2s(),
		new Int4s(),
		new Int8s(),
		new Numerics(),
		new Names(),
		new Strings(),
		new Dates(),
		new TimesWithoutTZ(),
		new TimesWithTZ(),
		new Timestamps(),
		new UUIDs(),
		new Moneys(),
		new Arrays(),
		new Records(),
	};
	
	private static final Unsupporteds UNSUPPORTEDS = new Unsupporteds(); 

	public static TextIO loadNamerTextIO(String baseName, Context context) {
		TextIO io = new TextIO();
		io.encoder = loadTextEncoderProc(baseName + "in", context);
		io.decoder = loadTextDecoderProc(baseName + "out", context);
		return io;
	}

	public static TextIO.Encoder loadTextEncoderProc(String name, Context context) {

		TextIO.Encoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findTextEncoder(name, context)) != null)
				return h;
		}

		return UNSUPPORTEDS.findTextEncoder(name, context);
	}

	public static TextIO.Decoder loadTextDecoderProc(String name, Context context) {
		
		TextIO.Decoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findTextDecoder(name, context)) != null)
				return h;
		}
		
		return UNSUPPORTEDS.findTextDecoder(name, context);
	}
	
	public static BinaryIO loadNamedBinaryIO(String baseName, Context context) {
		BinaryIO io = new BinaryIO();
		io.encoder = loadBinaryEncoderProc(baseName + "recv", context);
		io.decoder = loadBinaryDecoderProc(baseName + "send", context);
		return io;
	}

	public static BinaryIO.Encoder loadBinaryEncoderProc(String name, Context context) {
		
		BinaryIO.Encoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findBinaryEncoder(name, context)) != null)
				return h;
		}

		return UNSUPPORTEDS.findBinaryEncoder(name, context);
	}

	public static BinaryIO.Decoder loadBinaryDecoderProc(String name, Context context) {
		
		BinaryIO.Decoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findBinaryDecoder(name, context)) != null)
				return h;
		}

		return UNSUPPORTEDS.findBinaryDecoder(name, context);
	}

}
