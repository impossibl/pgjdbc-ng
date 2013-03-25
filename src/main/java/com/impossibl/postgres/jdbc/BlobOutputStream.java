package com.impossibl.postgres.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

public class BlobOutputStream extends OutputStream {
	
	LargeObject lo;
	byte[] buf;
	int pos;

	public BlobOutputStream(LargeObject lo) {
		super();
		this.lo = lo;
	}


	@Override
	public void write(int b) throws IOException {
		
		if(pos >= buf.length) {
			writeNextRegion();
		}
		
		buf[pos++] = (byte)b;
	}


	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}


	@Override
	public void write(byte[] b, int off, int len) throws IOException {

		if(pos > 0) {
			writeNextRegion();
		}
		
		try {
			lo.write(b, off, len);
		}
		catch(SQLException e) {
			throw new IOException(e);
		}
		
	}

	private void writeNextRegion() throws IOException {
		
		try {
			lo.write(buf, 0, pos);
			pos = 0;
		}
		catch(SQLException e) {
			throw new IOException(e);
		}
		
	}
	
}
