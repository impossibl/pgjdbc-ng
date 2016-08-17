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

import static com.impossibl.postgres.types.PrimitiveType.Numeric;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

public class Numerics extends SimpleProcProvider {

  private static final short NUMERIC_POS = (short) 0x0000;
  private static final short NUMERIC_NEG = (short) 0x4000;
  private static final short DEC_DIGITS = 4;

  public Numerics() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "numeric_");
  }

  private static Number convertStringInput(String value) {
    if (value.equalsIgnoreCase("NaN")) {
      return Double.NaN;
    }
    if (value.equalsIgnoreCase("infinity") || value.equalsIgnoreCase("+infinity")) {
      return Double.POSITIVE_INFINITY;
    }
    if (value.equalsIgnoreCase("-infinity")) {
      return Double.POSITIVE_INFINITY;
    }
    return new BigDecimal(value);
  }

  private static BigDecimal convertBoolInput(Boolean value) {
    return value ? BigDecimal.ONE : BigDecimal.ZERO;
  }

  private static Number convertInput(Number source) {

    if (source instanceof BigDecimal) {
      return source;
    }

    if (source instanceof BigInteger) {
      return new BigDecimal(source.toString());
    }

    if (source instanceof Byte || source instanceof Short || source instanceof Integer || source instanceof Long) {
      return BigDecimal.valueOf(source.longValue());
    }

    if (source instanceof AtomicInteger) {
      return new BigDecimal(((AtomicInteger) source).get());
    }

    if (source instanceof AtomicLong) {
      return new BigDecimal(((AtomicLong) source).get());
    }

    if (source instanceof Float) {
      Float source1 = (Float) source;
      if (source1.isNaN() || source1.isInfinite()) {
        return source1.doubleValue();
      }
      return BigDecimal.valueOf(source1);
    }

    if (source instanceof Double) {
      Double source1 = (Double) source;
      if (source1.isNaN() || source1.isInfinite()) {
        return source1;
      }
      return BigDecimal.valueOf(source1);
    }

    return null;
  }

  private static String convertStringOutput(Number value) {
    return value.toString();
  }

  static class BinDecoder extends NumericBinaryDecoder<Number> {

    BinDecoder() {
      super(null, Numerics::convertStringOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Numeric;
    }

    @Override
    public Class<Number> getDefaultClass() {
      return Number.class;
    }

    @Override
    protected Number decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      int readStart = buffer.readerIndex();

      short digitCount = buffer.readShort();
      short[] info = new short[3];
      info[0] = buffer.readShort(); //weight
      info[1] = buffer.readShort(); //sign
      info[2] = buffer.readShort(); //displayScale

      if (info[0] == 0 && info[1] == -16384 && info[2] == 0) {
        return Double.NaN;
      }

      short[] digits = new short[digitCount];
      for (int d = 0; d < digits.length; ++d)
        digits[d] = buffer.readShort();

      String num = decodeToString(info[0], info[1], info[2], digits);

      if (length != buffer.readerIndex() - readStart) {
        throw new IOException("invalid length");
      }

      return new BigDecimal(num);
    }

  }

  static class BinEncoder extends NumericBinaryEncoder<Number> {

    BinEncoder() {
      super(null, Numerics::convertStringInput, Numerics::convertBoolInput, Numerics::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Numeric;
    }

    @Override
    public Class<Number> getDefaultClass() {
      return Number.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Number value, Object sourceContext, ByteBuf buffer) throws IOException {

      if (Double.isNaN(value.doubleValue())) {
        buffer.writeShort(0);
        buffer.writeShort(0);
        buffer.writeShort(-16384);
        buffer.writeShort(0);
        return;
      }

      BigDecimal decimal = (BigDecimal) value;
      if (sourceContext != null) {
        int scale = ((Number)sourceContext).intValue();
        decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
      }

      String num = decimal.toPlainString();

      short[] info = new short[3];
      short[] digits = encodeFromString(num, info);

      buffer.writeShort(digits.length);

      buffer.writeShort(info[0]); //weight
      buffer.writeShort(info[1]); //sign
      buffer.writeShort(info[2]); //displayScale

      for (short digit : digits) {
        buffer.writeShort(digit);
      }

    }

  }

  static class TxtDecoder extends NumericTextDecoder<Number> {

    protected TxtDecoder() {
      super(Numerics::convertStringOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Numeric;
    }

    @Override
    public Class<Number> getDefaultClass() {
      return Number.class;
    }

    @Override
    protected Number decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {
      return context.getDecimalFormatter().parse(buffer.toString());
    }

  }

  static class TxtEncoder extends NumericTextEncoder<Number> {

    TxtEncoder() {
      super(Numerics::convertStringInput, Numerics::convertBoolInput, Numerics::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Numeric;
    }

    @Override
    public Class<Number> getDefaultClass() {
      return Number.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Number value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(context.getDecimalFormatter().format(value));
    }

  }

  /**
   * Encodes a string of the plain form xxxx.xxx into an NBASE packed sequence
   * of shorts.
   */
  private static short[] encodeFromString(String num, short[] info) {

    char[] numChars = num.toCharArray();
    byte[] numDigs = new byte[numChars.length - 1 + DEC_DIGITS * 2];
    int ch = 0;
    int digs = DEC_DIGITS;
    boolean haveDP = false;

    //Swallow leading zeros
    while (ch < numChars.length && numChars[ch] == '0')
      ch++;

    short sign = NUMERIC_POS;
    short displayWeight = -1;
    short displayScale = 0;

    if (ch < numChars.length && numChars[ch] == '-') {
      sign = NUMERIC_NEG;
      ++ch;
    }

    /*
     * Copy to array of single byte digits
     */

    while (ch < numChars.length) {

      if (numChars[ch] == '.') {

        haveDP = true;
        ch++;
      }
      else {

        numDigs[digs++] = (byte) (numChars[ch++] - '0');
        if (!haveDP)
          displayWeight++;
        else
          displayScale++;
      }

    }

    digs -= DEC_DIGITS;

    /*
     * Pack into NBASE format
     */

    short weight;

    if (displayWeight >= 0)
      weight = (short) ((displayWeight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1);
    else
      weight = (short) -((-displayWeight - 1) / DEC_DIGITS + 1);

    int offset = (weight + 1) * DEC_DIGITS - (displayWeight + 1);
    int digitCount = (digs + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    int i = DEC_DIGITS - offset;
    short[] digits = new short[digitCount];
    int d = 0;

    while (digitCount-- > 0) {
      digits[d++] = (short) (((numDigs[i] * 10 + numDigs[i + 1]) * 10 + numDigs[i + 2]) * 10 + numDigs[i + 3]);
      i += DEC_DIGITS;
    }

    info[0] = weight;
    info[1] = sign;
    info[2] = displayScale;
    return digits;
  }

  /**
   * Decodes a sequence of digits NBASE packed in shorts into a string of the
   * plain form xxxx.xxx
   */
  private static String decodeToString(short weight, short sign, short displayScale, short[] digits) {

    StringBuilder sb = new StringBuilder();

    if (sign == NUMERIC_NEG) {
      sb.append('-');
    }

    /*
     * Digits before decimal
     */
    int d;

    if (weight < 0) {
      d = weight + 1;
      sb.append(0);
    }
    else {

      for (d = 0; d <= weight; d++) {

        short dig = d < digits.length ? digits[d] : 0;
        boolean putIt = (d > 0);

        for (int b = 1000; b > 1; b /= 10) {

          short d1 = (short) (dig / b);
          dig -= d1 * b;
          putIt |= d1 > 0;
          if (putIt)
            sb.append((char) (d1 + '0'));
        }

        sb.append((char) (dig + '0'));

      }

    }

    /*
     * Digits after decimal
     */

    if (displayScale > 0) {

      sb.append('.');

      int length = sb.length() + displayScale;

      for (int i = 0; i < displayScale; d++, i += DEC_DIGITS) {

        short dig = (d >= 0 && d < digits.length) ? digits[d] : 0;

        for (int b = 1000; b > 1 && sb.length() < length; b /= 10) {

          short d1 = (short) (dig / b);
          dig -= d1 * b;
          sb.append((char) (d1 + '0'));
        }

        if (sb.length() < length)
          sb.append((char) (dig + '0'));

      }

    }
    return sb.toString();
  }

}
