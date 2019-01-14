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
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.api.jdbc.PGType.MACADDR8;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class MacAddr8s extends SimpleProcProvider {

  // Matches PostgreSQL procedures in source: src/backend/utils/adt/mac8.c

  public MacAddr8s() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "macaddr8_");
  }

  private static byte[] convertInput(Type type, Object value) throws ConversionException {

    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return parse((String) value);
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertOutput(Type type, byte[] value, Class<?> targetClass) throws ConversionException {

    if (targetClass == byte[].class) {
      return value;
    }

    if (targetClass == String.class) {
      StringBuilder bldr = new StringBuilder();
      MacAddrs.format(value, bldr);
      return bldr.toString();
    }

    throw new ConversionException(type, targetClass);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(8);
    }

    @Override
    public Class<?> getDefaultClass() {
      return MACADDR8.getJavaType();
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      // The external representation is just the eight bytes, MSB first.
      byte[] bytes = new byte[8];
      buffer.readBytes(bytes);

      return convertOutput(type, bytes, targetClass);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      byte[] bytes = convertInput(type, value);

      buffer.writeBytes(bytes);
    }
  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return MACADDR8.getJavaType();
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      byte[] value = parse(buffer.toString());

      return convertOutput(type, value, targetClass);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      byte[] addr = convertInput(type, value);

      MacAddrs.format(addr, buffer);
    }

  }

  /*
   * '08:00:2b:01:02:03:07:08' '08-00-2b-01-02-03-07-08' '0800:2b01:0203:0708' '08002b-0102-030708'
   * '08002b.0102.030708' '08002b0102030708'
   */
  static byte[] parse(String value) throws ConversionException {

    byte[] bytes = Bytes.decodeHex(value, true);
    if (bytes.length != 8) {
      throw new ConversionException("Invalid Mac address: " + value);
    }

    return bytes;
  }

}
