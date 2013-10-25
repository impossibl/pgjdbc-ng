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

        if(len == left && buf.length == 0)
          return -1;
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
