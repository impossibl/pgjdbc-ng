package com.impossibl.postgres.codecs;

import java.nio.charset.Charset;


public class StringCodec implements Codec<String> {

	Charset charset;

	public StringCodec(Charset charset) {
		this.charset = charset;
	}

	public String decode(byte[] bytes) {
		return new String(bytes, charset);
	}

	public byte[] encode(String string) {
		return string.getBytes(charset);
	}

	public Charset getCharset() {
		return charset;
	}

	public void setCharset(Charset charset) {
		this.charset = charset;
	}

}
