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
package com.impossibl.postgres.jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;



public class RandomInputStream extends InputStream {

  private Random generator = new Random();
  private boolean closed = false;

  @Override
  public int read() throws IOException {
    checkOpen();
    int result = generator.nextInt() % 256;
    if (result < 0) {
      result = -result;
    }
    return result;
  }

  @Override
  public int read(byte[] data, int offset, int length) throws IOException {
    checkOpen();
    byte[] temp = new byte[length];
    generator.nextBytes(temp);
    System.arraycopy(temp, 0, data, offset, length);
    return length;

  }

  @Override
  public int read(byte[] data) throws IOException {
    checkOpen();
    generator.nextBytes(data);
    return data.length;

  }

  @Override
  public long skip(long bytesToSkip) throws IOException {
    checkOpen();
    // It's all random so skipping has no effect.
    return bytesToSkip;
  }

  @Override
  public void close() {
    this.closed = true;
  }

  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("Input stream closed");
    }
  }

  @Override
  public int available() {
    // Limited only by available memory and the size of an array.
    return Integer.MAX_VALUE;
  }

}
