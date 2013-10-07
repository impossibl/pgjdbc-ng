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

import java.sql.SQLException;

class LargeObject {
	
	protected static final int INV_READ 	= 0x00040000;
	protected static final int INV_WRITE 	= 0x00020000;
	
	protected static final int SEEK_SET = 0;
	protected static final int SEEK_CUR = 1;
	protected static final int SEEK_END = 2;
	
	int oid;
	int fd;
	PGConnection connection;
	
	static LargeObject open(PGConnection connection, int oid) throws SQLException {
		int fd = open(connection, oid, INV_READ|INV_WRITE);
		if(fd == -1) {
			throw new SQLException("Unable to open large object");
		}
		
		if(connection.getServerVersion().compatible(9, 3, null))
			return new LargeObject64(connection, oid, fd);
		else
			return new LargeObject(connection, oid, fd);
	}
	
	LargeObject(PGConnection connection, int oid, int fd) {
		super();
		this.oid = oid;
		this.fd = fd;
		this.connection = connection;
	}

	LargeObject dup() throws SQLException {
		return open(connection, oid);
	}

	static int creat(PGConnection conn, int mode) throws SQLException {
		return conn.executeForResult("select lo_creat($1)", true, Integer.class, mode);
	}
	
	static int open(PGConnection conn, int oid, int access) throws SQLException {
		return conn.executeForResult("select lo_open($1,$2)", true, Integer.class, oid, access);
	}
	
	static int unlink(PGConnection conn, int oid) throws SQLException {
		return conn.executeForResult("select lo_unlink($1)", true, Integer.class, oid);
	}

	int close() throws SQLException {
		return connection.executeForResult("select lo_close($1)", true, Integer.class, fd);
	}
	
	long lseek(long offset, int whence) throws SQLException {
		return connection.executeForResult("select lo_lseek($1,$2,$3)", true, Integer.class, fd, (int)offset, whence);		
	}
	
	long tell() throws SQLException {
		return connection.executeForResult("select lo_tell($1)", true, Integer.class, fd);		
	}

	byte[] read(long len) throws SQLException {
		return connection.executeForResult("select loread($1,$2)", true, byte[].class, fd, (int)len);		
	}

	int write(byte[] data, int off, int len) throws SQLException {

		//TODO optimize away by supporting passing of array sections as parameters
		if(off != 0 || len != data.length) {
			byte[] sub = new byte[len];
			System.arraycopy(data, off, sub, 0, len);
			data = sub;
		}
		
		return connection.executeForResult("select lowrite($1,$2)", true, Integer.class, fd, data);		
	}
	
	int truncate(long len) throws SQLException {
		return connection.executeForResult("select lo_truncate($1,$2)", true, Integer.class, fd, (int)len);		
	}
	
}
