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

import com.impossibl.postgres.api.data.Tid;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;



public class Tids extends SimpleProcProvider {

  public Tids() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "tid");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<?> getOutputType() {
      return Tid.class;
    }

    @Override
    Object decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 6) {
        throw new IOException("invalid length");
      }

      int block = buffer.readInt();
      short offset = buffer.readShort();

      return new Tid(block, offset);
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<?> getInputType() {
      return Tid.class;
    }

    @Override
    void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        Tid tid = (Tid) val;

        buffer.writeInt(6);
        buffer.writeInt(tid.block);
        buffer.writeShort(tid.offset);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {
      return val == null ? 4 : 10;
    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<?> getOutputType() {
      return Tid.class;
    }

    @Override
    Tid decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      String[] items = buffer.subSequence(1, buffer.length() - 1).toString().split(",");

      int block = Integer.parseInt(items[0]);
      short offset = Short.parseShort(items[1]);

      return new Tid(block, offset);
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<?> getInputType() {
      return Tid.class;
    }

    @Override
    void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      Tid tid = (Tid) val;

      buffer.append('(').append(tid.block).append(',').append(tid.offset).append(')');
    }

  }

}
