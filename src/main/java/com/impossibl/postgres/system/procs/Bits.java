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
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.PrimitiveType.Bits;

import java.io.IOException;
import java.util.BitSet;

import org.jboss.netty.buffer.ChannelBuffer;

public class Bits extends SimpleProcProvider {

  public Bits() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "bit_", "varbit_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Bits;
    }

    @Override
    public Class<?> getOutputType() {
      return BitSet.class;
    }

    @Override
    public BitSet decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }

      int bitCount = buffer.readInt();
      int byteCount = (bitCount + 7) / 8;

      byte[] bytes = new byte[byteCount];
      buffer.readBytes(bytes);

      // Set equivalent bits in bit set (they use reversed encodings so
      // they cannot be just copied in
      BitSet bs = new BitSet(bitCount);
      for (int c = 0; c < bitCount; ++c) {
        bs.set(c, (bytes[c / 8] & (0x80 >> (c % 8))) != 0);
      }

      return bs;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return BitSet.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Bits;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        BitSet bs = (BitSet) val;

        int bitCount = bs.size();
        int byteCount = (bitCount + 7) / 8;

        // Set equivalent bits in byte array (they use reversed encodings so
        // they cannot be just copied in
        byte[] bytes = new byte[byteCount];
        for (int c = 0; c < bitCount; ++c) {
          bytes[c / 8] |= ((0x80 >> (c % 8)) & (bs.get(c) ? 0xff : 0x00));
        }

        buffer.writeInt(bs.size());
        buffer.writeBytes(bytes);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {

      if (val == null)
        return 4;

      BitSet bs = (BitSet) val;

      int bitCount = bs.size();
      int byteCount = (bitCount + 7) / 8;

      return 4 + byteCount;
    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Bits;
    }

    @Override
    public Class<?> getOutputType() {
      return BitSet.class;
    }

    @Override
    public BitSet decode(Type type, CharSequence buffer, Context context) throws IOException {

      BitSet bits = new BitSet();

      for (int c = 0, sz = buffer.length(); c < sz; ++c) {

        switch(buffer.charAt(c)) {
          case '0':
            bits.clear(c);
            break;

          case '1':
            bits.set(c);
            break;

          default:
            throw new IOException("Invalid bits format");
        }
      }

      return bits;
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return BitSet.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Bits;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      BitSet bits = (BitSet) val;

      for (int c = 0, sz = bits.length(); c < sz; ++c) {

        buffer.append(bits.get(c) ? '1' : '0');
      }

    }

  }

}
