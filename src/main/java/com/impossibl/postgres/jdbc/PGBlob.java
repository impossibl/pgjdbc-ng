package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_CONNECTION;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PGBlob implements Blob {
	
  private class LOByteIterator
  {
      private static final int MAX_BUFFER_SIZE = 8096;
      private byte buffer[] = {};
      private int idx = 0;

      LOByteIterator(long start) throws SQLException {
          lo.lseek(start, LargeObject.SEEK_SET);
      }

      boolean hasNext() throws SQLException
      {
          boolean result = false;
          if(idx < buffer.length) {
              result = true;
          }
          else {
              buffer = lo.read(MAX_BUFFER_SIZE);
              idx = 0;
              result = (buffer.length > 0);
          }
          return result;
      }

      private byte next()
      {
          return buffer[idx++];
      }
  }	


  LargeObject lo;
	List<LargeObject> streamLos;

	public PGBlob(PGConnection connection, int oid) throws SQLException {
		lo = LargeObject.open(connection, oid);
		streamLos = new ArrayList<>();
	}
	
	private void checkClosed() throws SQLException {
		if(lo == null) {
			throw CLOSED_CONNECTION;
		}
	}

	@Override
	public long length() throws SQLException {
		checkClosed();

		return lo.tell();
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		checkClosed();

		lo.lseek(pos, LargeObject.SEEK_SET);
		return lo.read(length);
	}

	@Override
	public InputStream getBinaryStream() throws SQLException {
		checkClosed();

		LargeObject streamLo = lo.dup();
		streamLos.add(streamLo);
		return new BlobInputStream(streamLo);
	}

	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		checkClosed();

		LargeObject streamLo = lo.dup();
		streamLos.add(streamLo);
		streamLo.lseek(pos, LargeObject.SEEK_SET);
		return new BlobInputStream(streamLo, length);
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		checkClosed();

		LOByteIterator iter = new LOByteIterator(start);
		long curPos=start, matchStartPos=0;
		int patternIdx=0;
		
		while(iter.hasNext()) {
			
			byte b = iter.next();
			
			if(b == pattern[patternIdx]) {
				
				if(patternIdx == 0) {
					matchStartPos = curPos;
				}
				
				patternIdx++;
				
				if(patternIdx == pattern.length) {
					return matchStartPos;
				}
			}
			else {
				patternIdx = 0;
			}
			
		}

		return -1;
	}
	
	@Override
	public long position(Blob pattern, long start) throws SQLException {
		checkClosed();

		return position(pattern.getBytes(0, (int)pattern.length()), start);
	}

	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		checkClosed();

		return setBytes(pos, bytes, 0, bytes.length);
	}

	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		checkClosed();

		lo.lseek(pos, LargeObject.SEEK_SET);
		return lo.write(bytes, offset, len);
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		checkClosed();

		LargeObject streamLo = lo.dup();
		streamLos.add(streamLo);
		streamLo.lseek(pos, LargeObject.SEEK_SET);
		return new BlobOutputStream(streamLo);
	}

	@Override
	public void truncate(long len) throws SQLException {
		checkClosed();

		lo.truncate(len);
	}

	@Override
	public void free() throws SQLException {
		checkClosed();

		lo.close();
		for(LargeObject streamLo : streamLos) {
			streamLo.close();
		}
		streamLos.clear();
	}

}
