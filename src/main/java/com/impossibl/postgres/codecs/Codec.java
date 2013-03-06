package com.impossibl.postgres.codecs;

/*
 * Simple interface for encoding and decoding values
 * to/from binary data.
 * 
 * Codecs are generally used for values that depend
 * on server or client settings to generate the correct
 * values.
 */
public interface Codec<T> {

	T decode(byte[] value);

	byte[] encode(String value);

}
