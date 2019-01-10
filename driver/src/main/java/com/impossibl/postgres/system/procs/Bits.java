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
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;

import io.netty.buffer.ByteBuf;

public class Bits extends SimpleProcProvider {

  public Bits() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "bit_", "varbit_");
  }

  static boolean[] convertInput(Context context, Object source, Object sourceContext) throws ConversionException {

    if (source instanceof BitSet) {
      BitSet val = (BitSet) source;
      boolean[] res = new boolean[val.size()];
      for (int bitIdx = 0; bitIdx < res.length; ++bitIdx) {
        res[bitIdx] = val.get(bitIdx);
      }
      return res;
    }

    if (source instanceof Boolean) {
      return new boolean[] {(Boolean) source};
    }

    if (source instanceof Byte) {
      Byte val = (Byte) source;
      return new boolean[] {val != 0};
    }

    if (source instanceof Short) {
      Short val = (Short) source;
      return new boolean[] {val != 0};
    }

    if (source instanceof Integer) {
      Integer val = (Integer) source;
      return new boolean[] {val != 0};
    }

    if (source instanceof Long) {
      Long val = (Long) source;
      return new boolean[] {val != 0};
    }

    if (source instanceof Float) {
      Float val = (Float) source;
      return new boolean[] {val != 0.0};
    }

    if (source instanceof Double) {
      Double val = (Double) source;
      return new boolean[] {val != 0.0};
    }

    if (source instanceof BigDecimal) {
      BigDecimal val = (BigDecimal) source;
      return new boolean[] {val.compareTo(BigDecimal.ZERO) != 0};
    }

    if (source instanceof String) {
      return bitStringToBools((String) source);
    }

    return null;
  }

  static Object convertOutput(Context context, boolean[] decoded, Class<?> targetClass, Object targetContext) throws ConversionException {

    if (targetClass == Boolean.class || targetClass == boolean.class) {
      return decoded[0];
    }

    if (targetClass == Byte.class || targetClass == byte.class) {
      return decoded[0] ? (byte) 1 : (byte) 0;
    }

    if (targetClass == Short.class || targetClass == short.class) {
      return decoded[0] ? (short) 1 : (short) 0;
    }

    if (targetClass == Integer.class || targetClass == int.class) {
      return decoded[0] ? 1 : 0;
    }

    if (targetClass == Long.class || targetClass == long.class) {
      return decoded[0] ? (long) 1 : (long) 0;
    }

    if (targetClass == BigInteger.class) {
      return decoded[0] ? BigInteger.ONE : BigInteger.ZERO;
    }

    if (targetClass == Float.class || targetClass == float.class) {
      return decoded[0] ? (float) 1.0 : (float) 0.0;
    }

    if (targetClass == Double.class || targetClass == double.class) {
      return decoded[0] ? 1.0 : 0.0;
    }

    if (targetClass == String.class) {
      StringBuilder val = new StringBuilder();
      boolsToBitString(decoded, val);
      return val.toString();
    }

    return null;
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<boolean[]> {

    BinDecoder() {
      super(Bits::convertOutput);
    }

    @Override
    public Class<boolean[]> getDefaultClass() {
      return boolean[].class;
    }

    @Override
    protected boolean[] decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int bitCount = buffer.readInt();

      if (typeModifier != null) {
        Integer lenMod = (Integer) type.getModifierParser().parse(typeModifier).get(Modifiers.LENGTH);
        if (lenMod > 0) {
          bitCount = lenMod;
        }
      }

      int byteCount = (bitCount + 7) / 8;

      byte[] bytes = new byte[byteCount];
      buffer.readBytes(bytes);

      // Set equivalent bits in bit set (they use reversed encodings so
      // they cannot be just copied in
      boolean[] bits = new boolean[bitCount];
      for (int c = 0; c < bitCount; ++c) {
        bits[c] = (bytes[c / 8] & (0x80 >> (c % 8))) != 0;
      }

      return bits;
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<boolean[]> {

    BinEncoder() {
      super(Bits::convertInput);
    }

    @Override
    protected Class<boolean[]> getDefaultClass() {
      return boolean[].class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, boolean[] value, Object sourceContext, ByteBuf buffer) throws IOException {

      int bitCount = value.length;
      int byteCount = (bitCount + 7) / 8;

      // Set equivalent bits in byte array (they use reversed encodings so
      // they cannot be just copied in
      byte[] bytes = new byte[byteCount];
      for (int c = 0; c < bitCount; ++c) {
        bytes[c / 8] |= ((0x80 >> (c % 8)) & (value[c] ? 0xff : 0x00));
      }

      buffer.writeInt(bitCount);
      buffer.writeBytes(bytes);
    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<boolean[]> {

    TxtDecoder() {
      super(Bits::convertOutput);
    }

    @Override
    public Class<boolean[]> getDefaultClass() {
      return boolean[].class;
    }

    @Override
    protected boolean[] decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      boolean[] bits = new boolean[buffer.length()];

      for (int c = 0, sz = buffer.length(); c < sz; ++c) {

        switch (buffer.charAt(c)) {
          case '0':
            bits[c] = false;
            break;

          case '1':
            bits[c] = true;
            break;

          default:
            throw new ConversionException("'" + buffer.charAt(c) + "' is not a valid binary digit");
        }
      }

      return bits;
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<boolean[]> {

    TxtEncoder() {
      super(Bits::convertInput);
    }

    @Override
    protected Class<boolean[]> getDefaultClass() {
      return boolean[].class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, boolean[] value, Object sourceContext, StringBuilder buffer) throws IOException {
      boolsToBitString(value, buffer);
    }

  }

  private static void boolsToBitString(boolean[] bits, Appendable out) throws ConversionException {
    try {
      for (boolean bit : bits) {
        out.append(bit ? '1' : '0');
      }
    }
    catch (IOException e) {
      throw new ConversionException("Error converting bits");
    }
  }

  private static boolean[] bitStringToBools(String val) throws ConversionException {

    if (val.length() == 0) {
      return new boolean[0];
    }

    switch (val.charAt(0)) {
      case 'B':
      case 'b':
        return binaryBitStringToBools(val.substring(1));

      case 'X':
      case 'x':
        return bytesToBools(Bytes.decodeHex(val.substring(1)));

      default:
        return binaryBitStringToBools(val);
    }

  }

  private static boolean[] bytesToBools(byte[] bytes) {
    BitSet bitSet = BitSet.valueOf(bytes);
    boolean[] bits = new boolean[bytes.length * 8];
    for (int c = 0; c < bits.length; ++c) {
      bits[bits.length - c - 1] = bitSet.get(c);
    }
    return bits;
  }

  private static boolean[] binaryBitStringToBools(String val) throws ConversionException {

    boolean[] bits = new boolean[val.length()];

    for (int c = 0; c < val.length(); ++c) {
      char cur = val.charAt(c);
      if (cur != '1' && cur != '0') {
        throw new ConversionException("'" + cur + "' is not a valid binary digit");
      }
      bits[c] = cur == '1';
    }

    return bits;
  }

}
