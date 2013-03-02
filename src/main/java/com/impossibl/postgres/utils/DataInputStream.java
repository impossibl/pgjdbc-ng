package com.impossibl.postgres.utils;

import java.io.DataInput;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


 public class DataInputStream extends FilterInputStream implements DataInput {

	private long count;
	private long mark = -1;
	private byte[] readBuffer;

	public DataInputStream(InputStream in) {
		super(in);
	}

	public long getCount() {
		return count;
	}

	@Override
	public int read() throws IOException {
		int result = in.read();
		if (result != -1) {
			count++;
		}
		return result;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int result = in.read(b, off, len);
		if (result != -1) {
			count += result;
		}
		return result;
	}

	@Override
	public long skip(long n) throws IOException {
		long result = in.skip(n);
		count += result;
		return result;
	}

	@Override
	public synchronized void mark(int readlimit) {
		in.mark(readlimit);
		mark = count;
		// it's okay to mark even if mark isn't supported, as reset won't work
	}

	@Override
	public synchronized void reset() throws IOException {
		if (!in.markSupported()) {
			throw new IOException("Mark not supported");
		}
		if (mark == -1) {
			throw new IOException("Mark not set");
		}

		in.reset();
		count = mark;
	}

	@Override
	public void readFully(byte b[]) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte b[], int off, int len) throws IOException {
		if (len < 0)
			throw new IndexOutOfBoundsException();
		int n = 0;
		while (n < len) {
			int count = read(b, off + n, len - n);
			if (count < 0)
				throw new EOFException();
			n += count;
		}
	}

	@Override
	public int skipBytes(int n) throws IOException {
		int total = 0;
		int cur = 0;

		while ((total < n) && ((cur = (int) in.skip(n - total)) > 0)) {
			total += cur;
		}

		return total;
	}

	@Override
	public boolean readBoolean() throws IOException {
		int ch = read();
		if (ch < 0)
			throw new EOFException();
		return (ch != 0);
	}

	@Override
	public byte readByte() throws IOException {
		int ch = read();
		if (ch < 0)
			throw new EOFException();
		return (byte) (ch);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		int ch = read();
		if (ch < 0)
			throw new EOFException();
		return ch;
	}

	@Override
	public short readShort() throws IOException {
		int ch1 = read();
		int ch2 = read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (short) ((ch1 << 8) + (ch2 << 0));
	}

	@Override
	public int readUnsignedShort() throws IOException {
		int ch1 = read();
		int ch2 = read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (ch1 << 8) + (ch2 << 0);
	}

	@Override
	public char readChar() throws IOException {
		int ch1 = read();
		int ch2 = read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (char) ((ch1 << 8) + (ch2 << 0));
	}

	@Override
	public int readInt() throws IOException {
		int ch1 = read();
		int ch2 = read();
		int ch3 = read();
		int ch4 = read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	@Override
	public long readLong() throws IOException {
    readFully(readBuffer, 0, 8);
    return (((long)readBuffer[0] << 56) +
            ((long)(readBuffer[1] & 255) << 48) +
            ((long)(readBuffer[2] & 255) << 40) +
            ((long)(readBuffer[3] & 255) << 32) +
            ((long)(readBuffer[4] & 255) << 24) +
            ((readBuffer[5] & 255) << 16) +
            ((readBuffer[6] & 255) <<  8) +
            ((readBuffer[7] & 255) <<  0));
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	public String readCString() throws IOException {
		StringBuilder sb = new StringBuilder();
		byte ch;
		while ((ch = readByte()) != 0)
			sb.append((char) ch);
		return sb.toString();
	}

}
