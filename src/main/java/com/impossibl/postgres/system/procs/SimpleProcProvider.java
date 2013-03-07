package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public class SimpleProcProvider extends BaseProcProvider {

	TextIO.Encoder txtEncoder;
	TextIO.Decoder txtDecoder;
	BinaryIO.Encoder binEncoder;
	BinaryIO.Decoder binDecoder;
	
	public SimpleProcProvider(TextIO.Encoder txtEncoder, TextIO.Decoder txtDecoder, BinaryIO.Encoder binEncoder, BinaryIO.Decoder binDecoder, String... baseNames) {
		super(baseNames);
		this.txtEncoder = txtEncoder;
		this.txtDecoder = txtDecoder;
		this.binEncoder = binEncoder;
		this.binDecoder = binDecoder;
	}

	public BinaryIO.Encoder findBinaryEncoder(String name, Context context) {
		if(hasName(name, "recv"))
			return binEncoder;
		return null;
	}

	public BinaryIO.Decoder findBinaryDecoder(String name, Context context) {
		if(hasName(name, "send"))
			return binDecoder;
		return null;
	}

	public TextIO.Encoder findTextEncoder(String name, Context context) {
		if(hasName(name, "in"))
			return txtEncoder;
		return null;
	}

	public TextIO.Decoder findTextDecoder(String name, Context context) {
		if(hasName(name, "out"))
			return txtDecoder;
		return null;
	}
	
}
