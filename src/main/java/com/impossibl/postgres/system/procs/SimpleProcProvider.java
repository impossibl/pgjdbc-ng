package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.Codec;



public class SimpleProcProvider extends BaseProcProvider {

	Codec.Encoder txtEncoder;
	Codec.Decoder txtDecoder;
	Codec.Encoder binEncoder;
	Codec.Decoder binDecoder;

	public SimpleProcProvider(Codec.Encoder txtEncoder, Codec.Decoder txtDecoder, Codec.Encoder binEncoder, Codec.Decoder binDecoder, String... baseNames) {
		super(baseNames);
		this.txtEncoder = txtEncoder;
		this.txtDecoder = txtDecoder;
		this.binEncoder = binEncoder;
		this.binDecoder = binDecoder;
	}

	public Codec.Encoder findEncoder(String name, Context context) {
		if(name.endsWith("recv") && hasName(name, "recv")) {
			return binEncoder;
		}
		else if(name.endsWith("in") && hasName(name, "in")) {
			return txtEncoder;
		}
		return null;
	}

	public Codec.Decoder findDecoder(String name, Context context) {
		if(name.endsWith("send") && hasName(name, "send")) {
			return binDecoder;
		}
		else if(name.endsWith("out") && hasName(name, "out")) {
			return txtDecoder;
		}
		return null;
	}

}
