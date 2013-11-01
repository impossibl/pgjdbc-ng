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
/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.impossibl.postgres.utils.guava;

import static com.impossibl.postgres.utils.guava.Preconditions.checkNotNull;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;

/**
 * Provides utility methods for working with character streams.
 *
 * <p>All method parameters must be non-null unless documented otherwise.
 *
 * <p>Some of the methods in this class take arguments with a generic type of
 * {@code Readable & Closeable}. A {@link java.io.Reader} implements both of
 * those interfaces. Similarly for {@code Appendable & Closeable} and
 * {@link java.io.Writer}.
 *
 * @author Chris Nokleberg
 * @author Bin Zhu
 * @author Colin Decker
 * @since 1.0
 */
public final class CharStreams {
  private static final int BUF_SIZE = 0x800; // 2K chars (4K bytes)

  private CharStreams() {
  }

  /**
   * Copies all characters between the {@link Readable} and {@link Appendable}
   * objects. Does not close or flush either object.
   *
   * @param from the object to read from
   * @param to the object to write to
   * @return the number of characters copied
   * @throws IOException if an I/O error occurs
   */
  public static long copy(Readable from, Appendable to) throws IOException {
    checkNotNull(from);
    checkNotNull(to);
    CharBuffer buf = CharBuffer.allocate(BUF_SIZE);
    long total = 0;
    while (from.read(buf) != -1) {
      buf.flip();
      to.append(buf);
      total += buf.remaining();
      buf.clear();
    }
    return total;
  }

  /**
   * Reads all characters from a {@link Readable} object into a {@link String}.
   * Does not close the {@code Readable}.
   *
   * @param r the object to read from
   * @return a string containing all the characters
   * @throws IOException if an I/O error occurs
   */
  public static String toString(Readable r) throws IOException {
    return toStringBuilder(r).toString();
  }

  /**
   * Reads all characters from a {@link Readable} object into a new
   * {@link StringBuilder} instance. Does not close the {@code Readable}.
   *
   * @param r the object to read from
   * @return a {@link StringBuilder} containing all the characters
   * @throws IOException if an I/O error occurs
   */
  private static StringBuilder toStringBuilder(Readable r) throws IOException {
    StringBuilder sb = new StringBuilder();
    copy(r, sb);
    return sb;
  }

  /**
   * Discards {@code n} characters of data from the reader. This method
   * will block until the full amount has been skipped. Does not close the
   * reader.
   *
   * @param reader the reader to read from
   * @param n the number of characters to skip
   * @throws EOFException if this stream reaches the end before skipping all
   *     the bytes
   * @throws IOException if an I/O error occurs
   */
  public static void skipFully(Reader reader, long n) throws IOException {
    checkNotNull(reader);
    while (n > 0) {
      long amt = reader.skip(n);
      if (amt == 0) {
        // force a blocking read
        if (reader.read() == -1) {
          throw new EOFException();
        }
        n--;
      }
      else {
        n -= amt;
      }
    }
  }

}
