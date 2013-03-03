package com.impossibl.postgres.utils;

import java.io.IOException;
import java.io.OutputStream;

public class LengthOutputStream extends OutputStream {
	
	long length;

	@Override
	public void write(int b) throws IOException {
		length += 1;
	}

	@Override
	public void write(byte[] b) throws IOException {
		length += b.length;
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		length += len;
	}

	public long getLength() {
		return length;
	}
	
}
