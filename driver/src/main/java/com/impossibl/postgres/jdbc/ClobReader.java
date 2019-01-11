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
import java.io.Reader;
import java.sql.SQLException;



public class ClobReader extends Reader {

  private static final int MAX_BUF_SIZE = 8 * 1024;

  PGClob owner;
  LargeObject lo;
  byte[] buf = {};
  int pos = 0;

  public ClobReader(PGClob owner, LargeObject lo) {
    this.owner = owner;
    this.lo = lo;
  }

  @Override
  public int read() throws IOException {

    if (pos >= buf.length) {
      readNextRegion();
    }

    return (pos < buf.length) ? ((buf[pos++] & 0xff) << 24 | (buf[pos++] & 0xff) << 16 | (buf[pos++] & 0xff) << 8 | (buf[pos++] & 0xff) << 0) : -1;
  }

  @Override
  public int read(char[] chars, int off, int len) throws IOException {
    if (chars == null) {
      throw new NullPointerException();
    }
    else if (off < 0 || len < 0 || len > chars.length - off) {
      throw new IndexOutOfBoundsException();
    }

    int left = len;
    while (left > 0) {

      int ch = read();
      if (ch == -1)
        break;

      chars[off++] = (char) ch;

      left--;
    }

    return len != left ? len - left : -1;
  }

  @Override
  public void close() throws IOException {
    if (lo == null) {
      return;
    }

    try {
      lo.close();
    }
    catch (SQLException e) {
      throw new IOException("Error closing stream", e);
    }
    if (owner != null) {
      owner.removeStream(lo);
    }
    owner = null;
    lo = null;
  }

  public void readNextRegion() throws IOException {
    try {
      buf = lo.read(MAX_BUF_SIZE);
      if (buf.length % PGClob.CHAR_SIZE != 0) {
        throw new IOException("invalid clob buffer read");
      }
      pos = 0;
    }
    catch (SQLException e) {
      throw new IOException(e);
    }
  }

}
