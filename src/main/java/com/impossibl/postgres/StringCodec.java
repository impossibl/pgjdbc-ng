package com.impossibl.postgres;

public interface StringCodec {

	String decode(byte[] bytes);
	byte[] encode(String string);
	
}
