package com.impossibl.postgres.codecs;

public interface Codec<T> {

	T decode(byte[] value);
	byte[] encode(String value);
	
}
