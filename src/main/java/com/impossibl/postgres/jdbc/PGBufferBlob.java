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

import com.impossibl.postgres.utils.guava.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

public class PGBufferBlob implements Blob {

  private ByteBuf buffer;

  public PGBufferBlob(ByteBuf buffer) {
    this.buffer = buffer;
  }

  private void checkFreed() throws SQLException {
    if (buffer == null) {
      throw new PGSQLSimpleException("Blob has been freed");
    }
  }

  private int offsetOf(long pos) throws SQLException {
    if (pos < 1 || pos > Integer.MAX_VALUE) {
      throw new PGSQLSimpleException("Blob offset is invalid");
    }

    int offset = (int) (pos - 1);
    if (offset >= buffer.capacity()) {
      throw new PGSQLSimpleException("Blob offset is invalid");
    }

    return offset;
  }

  private int availableOf(int offset, int length) {
    int endOffset = offset + length;
    return length - (endOffset - buffer.capacity());
  }

  @Override
  public long length() throws SQLException {
    checkFreed();

    return buffer.capacity();
  }

  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    checkFreed();
    int offset = offsetOf(pos);
    int avail = availableOf(offset, length);

    byte[] bytes = new byte[avail];
    buffer.getBytes(offset, bytes);
    return bytes;
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    checkFreed();

    return new ByteBufInputStream(buffer.retainedDuplicate(), true);
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    checkFreed();
    int bufferStart = offsetOf(start);

    while (bufferStart < buffer.capacity()) {
      int found = buffer.forEachByte(bufferStart, buffer.capacity() - bufferStart, new ByteProcessor.IndexOfProcessor(pattern[0]));
      if (found == -1) return -1;
      int avail = buffer.capacity() - found;
      if (avail < pattern.length) return -1;
      byte[] test = new byte[pattern.length];
      buffer.getBytes(found, test);
      if (Arrays.equals(pattern, test)) {
        return found;
      }
    }

    return -1;
  }

  @Override
  public long position(Blob pattern, long start) throws SQLException {
    checkFreed();

    try (InputStream in = pattern.getBinaryStream()) {
      return position(ByteStreams.toByteArray(in), start);
    }
    catch (IOException e) {
      throw new PGSQLSimpleException("Error reading blob", e);
    }
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    return setBytes(pos, bytes, 0, bytes.length);
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int bytesOff, int bytesLen) throws SQLException {
    checkFreed();
    if (bytesOff + bytesLen > bytes.length) {
      throw new PGSQLSimpleException("Invalid offset or length");
    }

    int offset = offsetOf(pos);
    int requiredLen = offset + bytes.length;

    if (requiredLen < buffer.capacity()) {
      buffer.capacity(requiredLen);
    }

    buffer.setBytes(offset, bytes);

    return bytes.length;
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    checkFreed();
    int offset = offsetOf(pos);

    return new ByteBufOutputStream(buffer.writerIndex(offset));
  }

  @Override
  public void truncate(long len) throws SQLException {
    checkFreed();

    buffer.capacity((int) len);
  }

  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    checkFreed();
    int offset = offsetOf(pos);
    int avail = availableOf(offset, (int) length);
    return new ByteBufInputStream(buffer.retainedSlice(offset, avail), true);
  }

  @Override
  public void free() {
    ReferenceCountUtil.release(buffer);
    buffer = null;
  }
}
