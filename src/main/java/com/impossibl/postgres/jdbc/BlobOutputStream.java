/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
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
		this.pos = 0;
		this.buf = new byte[1024];
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
