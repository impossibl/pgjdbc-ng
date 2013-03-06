package com.impossibl.postgres.codecs;

import java.nio.charset.Charset;



/*
 * Codec for Strings that uses a specific character
 * set to do perform the conversion.
 * 
 * This is a simple wrapper around String.getBytes
 * because others have noted performance penalties
 * when using String.getBytes. Although, after
 * investigation it is believed only to be with
 * older JVMs. This class exists mainly to ensure
 * easy replacement if that proves to be true at
 * a later time.
 */
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
