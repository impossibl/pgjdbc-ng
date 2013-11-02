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

import com.impossibl.postgres.data.Range;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.RangeType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.PrimitiveType.Range;
import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

public class Ranges extends SimpleProcProvider {

  public Ranges() {
    super(null, null, new Encoder(), new Decoder(), "range_");
  }

  static class Decoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Range;
    }

    @Override
    public Class<?> getOutputType() {
      return Object[].class;
    }

    @Override
    public Range<?> decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

      RangeType rangeType = (RangeType) type;
      Type baseType = rangeType.getBase();

      Range<?> instance = null;

      int length = buffer.readInt();

      if (length != -1) {

        Range.Flags flags = new Range.Flags(buffer.readByte());
        Object[] values = new Object[2];

        if (flags.hasLowerBound()) {

          values[0] = baseType.getBinaryCodec().decoder.decode(baseType, buffer, context);
        }

        if (flags.hasUpperBound()) {

          values[1] = baseType.getBinaryCodec().decoder.decode(baseType, buffer, context);
        }

        instance = new Range<Object>(flags, values);
      }

      return instance;
    }

  }

  static class Encoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Range.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Record;
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

}
