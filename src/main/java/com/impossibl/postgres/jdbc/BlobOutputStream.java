package com.impossibl.postgres.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

public class BlobOutputStream extends OutputStream {
	
	PGBlob owner;
	LargeObject lo;
	byte[] buf;
	int pos;

	public BlobOutputStream(PGBlob owner, LargeObject lo) {
		super();
		this.owner = owner;
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
	
	@Override
	public void flush() throws IOException {
		if(pos > 0) {
			writeNextRegion();
		}
	}
	
	@Override
	public void close() throws IOException {
		flush();
		try {
			lo.close();
		}
		catch (SQLException e) {
			throw new IOException("Error closing stream", e);
		}
		owner.removeStream(lo);
		owner = null;
		lo = null;
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
