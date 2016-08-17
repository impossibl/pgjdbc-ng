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
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.PrimitiveType.Bool;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import io.netty.buffer.ByteBuf;

public class Bools extends SimpleProcProvider {

  public Bools() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "bool");
  }

  @SuppressWarnings("unused")
  private static Boolean convertInput(Context context, Object source, Object sourceContext) throws ConversionException {

    if (source instanceof Boolean) {
      return (Boolean) source;
    }

    if (source instanceof Byte) {
      return (Byte) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof Short) {
      return (Short) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof Integer) {
      return (Integer) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof Long) {
      return (Long) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof BigInteger) {
      return ((BigInteger) source).compareTo(BigInteger.ZERO) != 0 ? TRUE : FALSE;
    }

    if (source instanceof Float) {
      return (Float) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof Double) {
      return (Double) source != 0 ? TRUE : FALSE;
    }

    if (source instanceof BigDecimal) {
      return ((BigDecimal) source).compareTo(BigDecimal.ZERO) != 0 ? TRUE : FALSE;
    }

    if (source instanceof Character) {
      switch ((Character) source) {
        case 'f':
        case 'F':
        case 'n':
        case 'N':
        case '0':
          return false;
        case 't':
        case 'T':
        case 'y':
        case 'Y':
        case '1':
          return true;
        default:
          throw new ConversionException("Cannot convert value '" + source + "' to bool");
      }
    }

    if (source instanceof String) {
      String val = (String) source;
      if (val.equalsIgnoreCase("f") ||
          val.equalsIgnoreCase("false") ||
          val.equalsIgnoreCase("n") ||
          val.equalsIgnoreCase("no") ||
          val.equalsIgnoreCase("off") ||
          val.equalsIgnoreCase("0"))
        return false;
      if (val.equalsIgnoreCase("t") ||
          val.equalsIgnoreCase("true") ||
          val.equalsIgnoreCase("y") ||
          val.equalsIgnoreCase("yes") ||
          val.equalsIgnoreCase("on") ||
          val.equalsIgnoreCase("1"))
        return true;
      throw new ConversionException("Cannot convert value \"" + val + "\" to bool");
    }

    return null;
  }

  @SuppressWarnings("unused")
  private static Object convertOutput(Context context, Boolean decoded, Class<?> targetClass, Object targetContext) {

    if (targetClass == Boolean.class || targetClass == boolean.class) {
      return decoded;
    }

    if (targetClass == Byte.class || targetClass == byte.class) {
      return decoded ? (byte)1 : (byte)0;
    }

    if (targetClass == Short.class || targetClass == short.class) {
      return decoded ? (short)1 : (short)0;
    }

    if (targetClass == Integer.class || targetClass == int.class) {
      return decoded ? 1 : 0;
    }

    if (targetClass == Long.class || targetClass == long.class) {
      return decoded ? (long)1 : (long)0;
    }

    if (targetClass == BigInteger.class) {
      return decoded ? BigInteger.ONE : BigInteger.ZERO;
    }

    if (targetClass == Float.class || targetClass == float.class) {
      return decoded ? (float)1.0 : (float)0.0;
    }

    if (targetClass == Double.class || targetClass == double.class) {
      return decoded ? 1.0 : 0.0;
    }

    if (targetClass == BigDecimal.class) {
      return decoded ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    if (targetClass == String.class) {
      return decoded ? "t" : "f";
    }

    return null;
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<Boolean> {

    BinDecoder() {
      super(1, Bools::convertOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Bool;
    }

    @Override
    public Class<Boolean> getDefaultClass() {
      return Boolean.class;
    }

    @Override
    protected Boolean decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {
      return buffer.readByte() != 0;
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<Boolean> {

    BinEncoder() {
      super(1, Bools::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Bool;
    }

    @Override
    public Class<Boolean> getDefaultClass() {
      return Boolean.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Boolean value, Object sourceContext, ByteBuf buffer) throws IOException {
      buffer.writeByte(value ? 1 : 0);
    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<Boolean> {

    TxtDecoder() {
      super(Bools::convertOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Bool;
    }

    @Override
    public Class<Boolean> getDefaultClass() {
      return Boolean.class;
    }

    @Override
    protected Boolean decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      switch (buffer.toString().toLowerCase()) {
        case "t":
        case "true":
        case "y":
        case "yes":
        case "on":
        case "1":
          return true;

        case "f":
        case "false":
        case "n":
        case "no":
        case "off":
        case "0":
          return false;
      }

      throw new ConversionException("Invalid format for boolean");
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<Boolean> {

    TxtEncoder() {
      super(Bools::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Bool;
    }

    @Override
    public Class<Boolean> getDefaultClass() {
      return Boolean.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Boolean value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(value ? "t" : "f");
    }

  }

}
