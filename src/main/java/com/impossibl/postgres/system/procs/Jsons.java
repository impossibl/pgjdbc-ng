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

import static com.impossibl.postgres.system.SystemSettings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.system.procs.Strings.TEXT_DECODER;
import static com.impossibl.postgres.system.procs.Strings.TEXT_ENCODER;
import static com.impossibl.postgres.types.PrimitiveType.String;

import java.io.IOException;

import static java.lang.Math.min;

import io.netty.buffer.ByteBuf;


public class Jsons extends SimpleProcProvider {

  public Jsons() {
    super(TEXT_ENCODER, TEXT_DECODER, new BinEncoder(), new BinDecoder(), "jsonb_");
  }

  public static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return String;
    }

    @Override
    public Class<?> getDefaultClass() {
      return String.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      if (length < 1) {
        throw new IOException("Invalid length for jsonb");
      }

      int version = buffer.readByte();
      if (version != 1) {
        throw new IOException("Invalid version for jsonb");
      }

      length -= 1;

      byte[] bytes;

      Integer maxLength = (Integer) context.getSetting(FIELD_VARYING_LENGTH_MAX);
      if (maxLength != null) {
        bytes = new byte[min(maxLength, length)];
      }
      else {
        bytes = new byte[length];
      }

      buffer.readBytes(bytes);
      buffer.skipBytes(length - bytes.length);

      return new String(bytes, context.getCharset());
    }

  }

  public static class BinEncoder extends BaseBinaryEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return String;
    }

    byte[] toBytes(Object val, Context context) {
      return val.toString().getBytes(context.getCharset());
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      byte[] bytes = toBytes(value, context);

      buffer.writeByte(1);

      buffer.writeBytes(bytes);
    }

  }

}
