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

import com.impossibl.postgres.data.Interval;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import static com.impossibl.postgres.types.PrimitiveType.Interval;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

public class Intervals extends SimpleProcProvider {

  public Intervals() {
    super(null, null, new Encoder(), new Decoder(), "interval_");
  }

  static class Decoder extends BinaryDecoder {

    public PrimitiveType getInputPrimitiveType() {
      return Interval;
    }

    public Class<?> getOutputType() {
      return Interval.class;
    }

    public Interval decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 16) {
        throw new IOException("invalid length");
      }

      long timeMicros = buffer.readLong();
      int days = buffer.readInt();
      int months = buffer.readInt();

      return new Interval(months, days, timeMicros);
    }

  }

  static class Encoder extends BinaryEncoder {

    public Class<?> getInputType() {
      return Interval.class;
    }

    public PrimitiveType getOutputPrimitiveType() {
      return Interval;
    }

    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        Interval interval = (Interval) val;

        buffer.writeInt(16);
        buffer.writeLong(interval.getRawTime());
        buffer.writeInt(interval.getRawDays());
        buffer.writeInt(interval.getRawMonths());
      }

    }

  }

}
