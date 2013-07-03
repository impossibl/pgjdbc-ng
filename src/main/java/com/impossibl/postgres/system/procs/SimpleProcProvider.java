package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type.Codec;



public class SimpleProcProvider extends BaseProcProvider {

	Codec.Encoder txtEncoder;
	Codec.Decoder txtDecoder;
	Codec.Encoder binEncoder;
	Codec.Decoder binDecoder;
	Modifiers.Parser modParser;

	public SimpleProcProvider(Codec.Encoder txtEncoder, Codec.Decoder txtDecoder, Codec.Encoder binEncoder, Codec.Decoder binDecoder, String... baseNames) {
		this(txtEncoder,txtDecoder,binEncoder,binDecoder,null, baseNames);
	}

	public SimpleProcProvider(Codec.Encoder txtEncoder, Codec.Decoder txtDecoder, Codec.Encoder binEncoder, Codec.Decoder binDecoder, Modifiers.Parser modParser, String... baseNames) {
		super(baseNames);
		this.txtEncoder = txtEncoder;
		this.txtDecoder = txtDecoder;
		this.binEncoder = binEncoder;
		this.binDecoder = binDecoder;
		this.modParser = modParser;
	}

	public SimpleProcProvider(Modifiers.Parser modParser, String... baseNames) {
		super(baseNames);
		this.modParser = modParser;
	}
	
	public Codec.Encoder findEncoder(String name, Context context) {
		if(name.endsWith("recv") && hasName(name, "recv", context)) {
			return binEncoder;
		}
		else if(name.endsWith("in") && hasName(name, "in", context)) {
			return txtEncoder;
		}
		return null;
	}

	public Codec.Decoder findDecoder(String name, Context context) {
		if(name.endsWith("send") && hasName(name, "send", context)) {
			return binDecoder;
		}
		else if(name.endsWith("out") && hasName(name, "out", context)) {
			return txtDecoder;
		}
		return null;
	}

	@Override
	public Modifiers.Parser findModifierParser(String name, Context context) {
		if(name.endsWith("typmodin") && hasName(name, "typmodin", context)) {
			return modParser;
		}
		if(name.endsWith("typmodout") && hasName(name, "typmodout", context)) {
			return modParser;
		}
		return null;
	}

}
