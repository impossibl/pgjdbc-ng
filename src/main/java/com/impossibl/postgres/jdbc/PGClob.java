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

import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.jdbc.Exceptions.CLOSED_CLOB;
import static com.impossibl.postgres.jdbc.Exceptions.ILLEGAL_ARGUMENT;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;



public class PGClob implements Clob {

  static final Charset CHARSET = Charset.forName("UTF_32BE");
  static final int CHAR_SIZE = 4;

  private class LOCharIterator {
    private static final int MAX_BUFFER_CHARS = 1024;
    private static final int MAX_BUFFER_SIZE = MAX_BUFFER_CHARS * CHAR_SIZE;
    private byte[] buffer = {};
    private int idx = 0;

    LOCharIterator(long start) throws SQLException {
      lo.lseek(start * CHAR_SIZE, LargeObject.SEEK_SET);
    }

    boolean hasNext() throws SQLException {
      boolean result;
      if (idx < buffer.length) {
        result = true;
      }
      else {
        buffer = lo.read(MAX_BUFFER_SIZE);
        idx = 0;
        result = (buffer.length > 0);
      }
      return result;
    }

    private int next() {
      return
          (buffer[idx++] & 0xff) << 24 |
          (buffer[idx++] & 0xff) << 16 |
          (buffer[idx++] & 0xff) << 8 |
          (buffer[idx++] & 0xff) << 0;
    }

  }


  LargeObject lo;
  List<LargeObject> streamLos;

  PGClob(PGConnectionImpl connection, int oid) throws SQLException {

    if (connection.getAutoCommit()) {
      throw new SQLException("Clobs require connection to be in manual-commit mode... setAutoCommit(false)");
    }

    lo = LargeObject.open(connection, oid);
    streamLos = new ArrayList<>();
  }

  private void checkClosed() throws SQLException {
    if (lo == null) {
      throw CLOSED_CLOB;
    }
  }

  private static void checkPosition(long pos) throws SQLException {
    if (pos < 1) {
      throw ILLEGAL_ARGUMENT;
    }
  }

  @Override
  public long length() throws SQLException {
    checkClosed();

    long cur = lo.tell();
    lo.lseek(0, LargeObject.SEEK_END);
    long len = lo.tell();
    lo.lseek(cur, LargeObject.SEEK_SET);
    return len / CHAR_SIZE;
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    checkClosed();
    checkPosition(pos);

    lo.lseek((pos - 1) * CHAR_SIZE, LargeObject.SEEK_SET);
    return new String(lo.read(length * CHAR_SIZE), CHARSET);
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    checkClosed();

    LargeObject streamLo = lo.dup();
    streamLos.add(streamLo);
    return new ClobReader(streamLo);
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    checkClosed();
    checkPosition(pos);

    LargeObject streamLo = lo.dup();
    streamLos.add(streamLo);
    streamLo.lseek((pos - 1) * CHAR_SIZE, LargeObject.SEEK_SET);
    return CharStreams.limit(new ClobReader(streamLo), length * CHAR_SIZE);
  }

  @Override
  public long position(String pattern, long start) throws SQLException {
    checkClosed();
    checkPosition(start);

    LOCharIterator iter = new LOCharIterator(start - 1);
    long curPos = start, matchStartPos = 0;
    int patternIdx = 0;

    while (iter.hasNext()) {

      int ch = iter.next();

      if (ch == pattern.charAt(patternIdx)) {

        if (patternIdx == 0) {
          matchStartPos = curPos;
        }

        patternIdx++;

        if (patternIdx == pattern.length()) {
          return matchStartPos;
        }
      }
      else {
        patternIdx = 0;
      }

      curPos++;
    }

    return -1;
  }

  @Override
  public long position(Clob pattern, long start) throws SQLException {
    checkClosed();

    return position(pattern.getSubString(1, (int) pattern.length()), start);
  }

  @Override
  public int setString(long pos, String str) throws SQLException {

    return setString(pos, str, 0, str.length());
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    checkClosed();
    checkPosition(pos);

    lo.lseek((pos - 1) * CHAR_SIZE, LargeObject.SEEK_SET);
    return lo.write(str.substring(offset, offset + len).getBytes(CHARSET), 0, len * CHAR_SIZE);
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    checkClosed();
    checkPosition(pos);

    LargeObject streamLo = lo.dup();
    streamLos.add(streamLo);
    streamLo.lseek((pos - 1) * CHAR_SIZE, LargeObject.SEEK_SET);
    return new ClobWriter(this, streamLo);
  }

  @Override
  public void truncate(long len) throws SQLException {
    checkClosed();

    lo.truncate(len * CHAR_SIZE);
  }

  @Override
  public void free() throws SQLException {

    if (lo == null)
      return;

    lo.close();
    lo = null;

    for (LargeObject streamLo : streamLos) {
      streamLo.close();
    }
    streamLos.clear();
  }

  void removeStream(LargeObject lo) {
    streamLos.remove(lo);
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}
