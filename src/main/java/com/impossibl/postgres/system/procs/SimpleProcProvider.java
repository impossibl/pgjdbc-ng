package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public class SimpleProcProvider implements ProcProvider {

	String[] baseNames;
	TextIO.Encoder txtEncoder;
	TextIO.Decoder txtDecoder;
	BinaryIO.Encoder binEncoder;
	BinaryIO.Decoder binDecoder;
	
	public SimpleProcProvider(TextIO.Encoder txtEncoder, TextIO.Decoder txtDecoder, BinaryIO.Encoder binEncoder, BinaryIO.Decoder binDecoder, String... baseNames) {
		super();
		this.baseNames = baseNames;
		this.txtEncoder = txtEncoder;
		this.txtDecoder = txtDecoder;
		this.binEncoder = binEncoder;
		this.binDecoder = binDecoder;
	}

	public BinaryIO.Encoder findBinaryEncoder(String name) {
		if(hasName(name, "recv"))
			return binEncoder;
		return null;
	}

	public BinaryIO.Decoder findBinaryDecoder(String name) {
		if(hasName(name, "send"))
			return binDecoder;
		return null;
	}

	public TextIO.Encoder findTextEncoder(String name) {
		if(hasName(name, "in"))
			return txtEncoder;
		return null;
	}

	public TextIO.Decoder findTextDecoder(String name) {
		if(hasName(name, "out"))
			return txtDecoder;
		return null;
	}
	
	private boolean hasName(String name, String suffix) {
		
		for(String baseName : baseNames) {
			if(name.equals(baseName+suffix))
				return true;
		}
		
		return false;
	}

}
