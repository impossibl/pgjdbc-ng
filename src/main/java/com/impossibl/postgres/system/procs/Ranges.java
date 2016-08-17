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

import com.impossibl.postgres.api.data.Range;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.RangeType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;

public class Ranges extends SimpleProcProvider {

  public Ranges() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "range_");
  }

  static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Range.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range.Flags flags = new Range.Flags(buffer.readByte());
      Object[] values = new Object[2];

      Type.Codec.Decoder<ByteBuf> decoder = baseType.getBinaryCodec().getDecoder();

      if (flags.hasLowerBound()) {

        values[0] = decoder.decode(context, baseType, null, null, buffer, targetClass, targetContext);
      }

      if (flags.hasUpperBound()) {

        values[1] = decoder.decode(context, baseType, null, null, buffer, targetClass, targetContext);
      }

      return new Range<>(flags, values);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range<?> range = (Range<?>) value;

      buffer.writeByte(range.getFlags().getValue());

      Type.Codec.Encoder<ByteBuf> encoder = baseType.getBinaryCodec().getEncoder();

      if (range.getFlags().hasLowerBound()) {

        encoder.encode(context, baseType, range.getLowerBound(), sourceContext, buffer);
      }

      if (range.getFlags().hasUpperBound()) {

        encoder.encode(context, baseType, range.getUpperBound(), sourceContext, buffer);
      }

    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Range.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      boolean lowerInc = false, upperInc = false;
      Object lower = null, upper = null;

      if (buffer.charAt(0) == '[') {
        lowerInc = true;
      }

      if (buffer.charAt(buffer.length() - 1) == ']') {
        upperInc = true;
      }

      Type.Codec.Decoder<CharSequence> decoder = baseType.getTextCodec().getDecoder();

      CharSequence lowerTxt = buffer.subSequence(1, findBound(buffer));
      if (lowerTxt.length() != 0) {
        lower = decoder.decode(context, baseType, null, null, lowerTxt, targetClass, targetContext);
      }

      CharSequence upperTxt = buffer.subSequence(2 + lowerTxt.length(), buffer.length() - 1);
      if (upperTxt.length() != 0) {
        upper = decoder.decode(context, baseType, null, null, upperTxt, targetClass, targetContext);
      }

      return Range.create(lower, lowerInc, upper, upperInc);
    }

    private static int findBound(CharSequence buffer) {

      boolean string = false;

      int stop;
      for (stop = 1; stop < buffer.length(); ++stop) {

        char ch = buffer.charAt(stop);
        switch (ch) {

          case '"':
            string = !string;
            break;

          case '\\':
            ++stop;
            break;

          default:

            if (ch == ',' && !string) {
              return stop;
            }

        }

      }

      return stop;
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range<?> range = (Range<?>) value;

      if (range.isLowerBoundInclusive()) {
        buffer.append('[');
      }
      else {
        buffer.append('(');
      }

      BaseTextEncoder encoder = (BaseTextEncoder) baseType.getTextCodec().getEncoder();

      if (range.hasLowerBound()) {
        StringBuilder lowerBuffer = new StringBuilder();
        encoder.encodeValue(context, baseType, range.getLowerBound(), sourceContext, lowerBuffer);
        String lower = lowerBuffer.toString();

        if (needsQuotes(lower)) {
          buffer.append('"').append(lower).append('"');
        }
        else {
          buffer.append(lower);
        }
      }

      buffer.append(',');

      if (range.hasUpperBound()) {
        StringBuilder upperBuffer = new StringBuilder();
        encoder.encodeValue(context, baseType, range.getUpperBound(), sourceContext, upperBuffer);
        String upper = upperBuffer.toString();

        if (needsQuotes(upper)) {
          buffer.append('"').append(upper).append('"');
        }
        else {
          buffer.append(upper);
        }
      }

      if (range.isUpperBoundInclusive()) {
        buffer.append(']');
      }
      else {
        buffer.append(')');
      }

    }

    private static boolean needsQuotes(String elemStr) {

      if (elemStr.isEmpty())
        return true;

      if (elemStr.equalsIgnoreCase("NULL"))
        return true;

      for (int c = 0; c < elemStr.length(); ++c) {

        char ch = elemStr.charAt(c);

        if (ch == '"' || ch == '\\' || ch == '{' || ch == '}' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
          return true;
      }

      return false;
    }

  }

}
