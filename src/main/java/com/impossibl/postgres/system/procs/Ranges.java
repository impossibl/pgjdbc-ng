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

import org.jboss.netty.buffer.ChannelBuffer;

public class Ranges extends SimpleProcProvider {

  public Ranges() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "range_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public Class<?> getOutputType() {
      return Range.class;
    }

    @Override
    public Range<?> decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range<?> instance = null;

      int length = buffer.readInt();

      if (length != -1) {

        Range.Flags flags = new Range.Flags(buffer.readByte());
        Object[] values = new Object[2];

        if (flags.hasLowerBound()) {

          values[0] = baseType.getBinaryCodec().decoder.decode(baseType, null, null, buffer, context);
        }

        if (flags.hasUpperBound()) {

          values[1] = baseType.getBinaryCodec().decoder.decode(baseType, null, null, buffer, context);
        }

        instance = new Range<Object>(flags, values);
      }

      return instance;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Range.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      buffer.writeInt(-1);

      if (val != null) {

        int writeStart = buffer.writerIndex();

        RangeType rangeType = (RangeType) type;
        Type baseType = rangeType.getBase();

        Range<?> range = (Range<?>) val;

        buffer.writeByte(range.getFlags().getValue());

        if (range.getFlags().hasLowerBound()) {

          baseType.getBinaryCodec().encoder.encode(baseType, buffer, range.getLowerBound(), context);
        }

        if (range.getFlags().hasUpperBound()) {

          baseType.getBinaryCodec().encoder.encode(baseType, buffer, range.getUpperBound(), context);
        }

        // Set length
        buffer.setInt(writeStart - 4, buffer.writerIndex() - writeStart);
      }

    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public Class<?> getOutputType() {
      return Range.class;
    }

    @Override
    public Range<?> decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

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

      CharSequence lowerTxt = buffer.subSequence(1, findBound(buffer, 1));
      if (lowerTxt.length() != 0) {
        lower = baseType.getTextCodec().decoder.decode(baseType, null, null, lowerTxt, context);
      }

      CharSequence upperTxt = buffer.subSequence(2 + lowerTxt.length(), buffer.length() - 1);
      if (upperTxt.length() != 0) {
        upper = baseType.getTextCodec().decoder.decode(baseType, null, null, upperTxt, context);
      }

      return Range.create(lower, lowerInc, upper, upperInc);
    }

    private int findBound(CharSequence buffer, int start) {

      boolean string = false;

      int stop;
      for (stop = start; stop < buffer.length(); ++stop) {

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

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Range.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Range;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range<?> range = (Range<?>) val;

      if (range.isLowerBoundInclusive()) {
        buffer.append('[');
      }
      else {
        buffer.append('(');
      }

      if (range.hasLowerBound()) {
        StringBuilder lowerBuffer = new StringBuilder();
        baseType.getTextCodec().encoder.encode(baseType, lowerBuffer, range.getLowerBound(), context);
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
        baseType.getTextCodec().encoder.encode(baseType, upperBuffer, range.getUpperBound(), context);
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

    private boolean needsQuotes(String elemStr) {

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
