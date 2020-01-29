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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;

public class UInt4s extends SimpleProcProvider {

  public UInt4s() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        "xid", "cid", "regproc", "regtype", "regclass", "regoper", "regnamespace", "regrole"
    );
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<Integer> {

    BinDecoder() {
      super(4, new DecodingConverter());
    }

    @Override
    public Class<Integer> getDefaultClass() {
      return Integer.class;
    }

    @Override
    protected Integer decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {
      return buffer.readInt();
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<Integer> {

    BinEncoder() {
      super(4, new EncodingConverter());
    }

    @Override
    public Class<Integer> getDefaultClass() {
      return Integer.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Integer value, Object sourceContext, ByteBuf buffer) throws IOException {
      buffer.writeInt(value);
    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<Integer> {

    TxtDecoder() {
      super(new DecodingConverter());
    }

    @Override
    public Class<Integer> getDefaultClass() {
      return Integer.class;
    }

    @Override
    protected Integer decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {
      return Integer.parseUnsignedInt(buffer.toString());
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<Integer> {

    TxtEncoder() {
      super(new EncodingConverter());
    }

    @Override
    public Class<Integer> getDefaultClass() {
      return Integer.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Integer value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(Integer.toUnsignedString(value));
    }

  }

  static class EncodingConverter implements AutoConvertingEncoder.Converter<Integer> {

    @Override
    public Integer convert(Context context, Object source, Object sourceContext) throws ConversionException {

      if (source instanceof String) {
        try {
          return Integer.parseUnsignedInt((String) source);
        }
        catch (NumberFormatException e) {
          throw new ConversionException("Invalid unsigned integer", e);
        }
      }

      if (source instanceof Boolean) {
        boolean boolVal = (boolean) source;
        return boolVal ? 1 : 0;
      }

      if (source instanceof Number) {
        Number numberVal = (Number) source;
        return (int) numberVal.longValue();
      }

      return null;
    }

  }

  static class DecodingConverter implements AutoConvertingDecoder.Converter<Integer> {

    @Override
    public Object convert(Context context, Integer decoded, Class<?> targetClass, Object targetContext) throws ConversionException {

      if (targetClass == String.class) {
        return Integer.toUnsignedString(decoded);
      }

      if (targetClass == BigDecimal.class) {
        long longVal = Integer.toUnsignedLong(decoded);
        return BigDecimal.valueOf(longVal);
      }

      if (targetClass == BigInteger.class) {
        long longVal = Integer.toUnsignedLong(decoded);
        return BigInteger.valueOf(longVal);
      }

      if (targetClass == Boolean.class || targetClass == boolean.class) {
        return decoded.byteValue() != 0 ? Boolean.TRUE : Boolean.FALSE;
      }

      if (targetClass == Byte.class || targetClass == byte.class) {
        long longVal = Integer.toUnsignedLong(decoded);
        if (longVal < Byte.MIN_VALUE || longVal > Byte.MAX_VALUE) {
          throw new ArithmeticException("Value out of byte range");
        }
        return (byte) longVal;
      }

      if (targetClass == Short.class || targetClass == short.class) {
        long longVal = Integer.toUnsignedLong(decoded);
        if (longVal < Short.MIN_VALUE || longVal > Short.MAX_VALUE) {
          throw new ArithmeticException("Value out of short range");
        }
        return (short) longVal;
      }

      if (targetClass == Integer.class || targetClass == int.class) {
        long longVal = Integer.toUnsignedLong(decoded);
        if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
          throw new ArithmeticException("Value out of int range");
        }
        return (int) longVal;
      }

      if (targetClass == Long.class || targetClass == long.class) {
        return Integer.toUnsignedLong(decoded);
      }

      return null;
    }

  }

}
