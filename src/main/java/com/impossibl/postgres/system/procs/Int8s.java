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
import static com.impossibl.postgres.types.PrimitiveType.Int8;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

public class Int8s extends SimpleProcProvider {

  public Int8s() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "int8");
  }

  static class BinDecoder extends BinaryDecoder {

    public PrimitiveType getInputPrimitiveType() {
      return Int8;
    }

    public Class<?> getOutputType() {
      return Long.class;
    }

    public Long decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 8) {
        throw new IOException("invalid length");
      }

      return buffer.readLong();
    }

  }

  static class BinEncoder extends BinaryEncoder {

    public Class<?> getInputType() {
      return Long.class;
    }

    public PrimitiveType getOutputPrimitiveType() {
      return Int8;
    }

    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        buffer.writeInt(8);
        buffer.writeLong((Long) val);
      }

    }

  }

  static class TxtDecoder extends TextDecoder {

    public PrimitiveType getInputPrimitiveType() {
      return Int8;
    }

    public Class<?> getOutputType() {
      return Long.class;
    }

    public Long decode(Type type, CharSequence buffer, Context context) throws IOException {

      return Long.valueOf(buffer.toString());
    }

  }

  static class TxtEncoder extends TextEncoder {

    public Class<?> getInputType() {
      return Long.class;
    }

    public PrimitiveType getOutputPrimitiveType() {
      return Int8;
    }

    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      buffer.append((long)val);
    }

  }

}
