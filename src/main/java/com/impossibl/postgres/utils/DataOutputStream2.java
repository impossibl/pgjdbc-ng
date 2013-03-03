/*
 * Copyright (c) 1994, 2004, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

package com.impossibl.postgres.utils;

import java.io.DataOutput;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStream2 extends FilterOutputStream implements DataOutput {

	protected long count;

	public DataOutputStream2(OutputStream out) {
		super(out);
	}

	private void incCount(long value) {
		long temp = count + value;
		if (temp < 0) {
			temp = Long.MAX_VALUE;
		}
		count = temp;
	}

	public synchronized void write(int b) throws IOException {
		out.write(b);
		incCount(1);
	}

	public synchronized void write(byte b[], int off, int len) throws IOException {
		out.write(b, off, len);
		incCount(len);
	}

	public void flush() throws IOException {
		out.flush();
	}

	public void writeBoolean(boolean v) throws IOException {
		out.write(v ? 1 : 0);
		incCount(1);
	}

	public void writeByte(int v) throws IOException {
		out.write(v);
		incCount(1);
	}

	public void writeShort(int v) throws IOException {
		out.write((v >>> 8) & 0xFF);
		out.write((v >>> 0) & 0xFF);
		incCount(2);
	}

	public void writeChar(int v) throws IOException {
		out.write((v >>> 8) & 0xFF);
		out.write((v >>> 0) & 0xFF);
		incCount(2);
	}

	public void writeInt(int v) throws IOException {
		out.write((v >>> 24) & 0xFF);
		out.write((v >>> 16) & 0xFF);
		out.write((v >>> 8) & 0xFF);
		out.write((v >>> 0) & 0xFF);
		incCount(4);
	}

	private byte writeBuffer[] = new byte[8];

	public void writeLong(long v) throws IOException {
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
		out.write(writeBuffer, 0, 8);
		incCount(8);
	}

	public void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	public void writeBytes(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; i++) {
			out.write((byte) s.charAt(i));
		}
		incCount(len);
	}

	public void writeChars(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; i++) {
			int v = s.charAt(i);
			out.write((v >>> 8) & 0xFF);
			out.write((v >>> 0) & 0xFF);
		}
		incCount(len * 2);
	}

	public void writeUTF(String str) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void writeCString(String str) throws IOException {
		for (int c = 0; c < str.length(); ++c)
			writeByte(str.charAt(c) & 0xFF);
		writeByte(0);
	}

	public long getCount() {
		return count;
	}
	
}
