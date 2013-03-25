package com.impossibl.postgres.jdbc;

import static java.lang.Math.min;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public class BlobInputStream extends InputStream {
	
	private static final int MAX_BUF_SIZE = 8 * 1024;
	
	LargeObject lo;
	byte[] buf = {};
	int pos = 0;
	
	public BlobInputStream(LargeObject lo) {
		this.lo = lo;
	}

	@Override
	public int read() throws IOException {
		
		if(pos >= buf.length) {
			readNextRegion();
		}
		
    return (pos < buf.length) ? (buf[pos++] & 0xff) : -1;
	}
  
	public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
        throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
    }

    int left = len;
    while(left > 0) {

	    if (pos >= buf.length) {
	    	readNextRegion();
	    }
	
	    int avail = buf.length - pos;
	    int amt = min(avail,  left);
	    if (amt <= 0) {
	        break;
	    }
	    
	    System.arraycopy(buf, pos, b, off+(len-left), amt);
	    pos += amt;
	    left -= amt;
    }
    
	  return len-left;
  }
	
	public void readNextRegion() throws IOException {
		try {
			buf = lo.read(MAX_BUF_SIZE);
			pos = 0;
		}
		catch(SQLException e) {
			throw new IOException(e);
		}
	}

}
