package com.impossibl.postgres;

import java.nio.charset.Charset;

public class BasicStringCodec implements StringCodec {
	
	Charset charset;

	public BasicStringCodec(Charset charset) {
		super();
		this.charset = charset;
	}

	public String decode(byte[] bytes) {
		return new String(bytes, charset);
	}

	public byte[] encode(String string) {
		return string.getBytes(charset);
	}

}
