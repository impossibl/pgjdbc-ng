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
		return conn.executeForResult("select lo_creat($1)", false, Integer.class, mode);
	}
	
	static int open(PGConnection conn, int oid, int access) throws SQLException {
		return conn.executeForResult("select lo_open($1,$2)", false, Integer.class, oid, access);
	}
	
	static int unlink(PGConnection conn, int oid) throws SQLException {
		return conn.executeForResult("select lo_unlink($1)", false, Integer.class, oid);
	}

	int close() throws SQLException {
		return connection.executeForResult("select lo_close($1)", false, Integer.class, fd);
	}
	
	int lseek(long offset, int whence) throws SQLException {
		return connection.executeForResult("select lo_lseek($1,$2,$3)", false, Integer.class, fd, (int)offset, whence);		
	}
	
	long tell() throws SQLException {
		return connection.executeForResult("select lo_tell($1)", false, Integer.class, fd);		
	}

	byte[] read(long len) throws SQLException {
		return connection.executeForResult("select loread($1,$2)", false, byte[].class, fd, (int)len);		
	}

	int write(byte[] data, int off, int len) throws SQLException {

		//TODO optimize away by supporting passing of array sections as parameters
		if(off != 0 || len != data.length) {
			byte[] sub = new byte[len];
			System.arraycopy(data, off, sub, 0, len);
			data = sub;
		}
		
		return connection.executeForResult("select lowrite($1,$2)", false, Integer.class, fd, data);		
	}
	
	int truncate(long len) throws SQLException {
		return connection.executeForResult("select lo_truncate($1,$2)", false, Integer.class, fd, (int)len);		
	}
	
}
