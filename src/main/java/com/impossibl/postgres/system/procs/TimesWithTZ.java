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

import com.impossibl.postgres.datetime.TimeZones;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.PreciseInstant;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.TimeTZ;

import java.io.IOException;
import java.util.TimeZone;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.jboss.netty.buffer.ChannelBuffer;

public class TimesWithTZ extends SettingSelectProcProvider {

  public TimesWithTZ() {
    super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
        null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
        null, null, null, null,
        "timetz_");
  }

  static class BinIntegerDecoder extends BinaryDecoder {

    public PrimitiveType getInputPrimitiveType() {
      return TimeTZ;
    }

    public Class<?> getOutputType() {
      return Instant.class;
    }

    public Instant decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 12) {
        throw new IOException("invalid length");
      }

      long micros = buffer.readLong();
      int tzOffsetSecs = buffer.readInt();

      int tzOffsetMillis = (int)SECONDS.toMillis(-tzOffsetSecs);
      TimeZone zone = TimeZones.getOffsetZone(tzOffsetMillis);

      return new PreciseInstant(Instant.Type.Time, micros, zone);
    }

  }

  static class BinIntegerEncoder extends BinaryEncoder {

    public Class<?> getInputType() {
      return Instant.class;
    }

    public PrimitiveType getOutputPrimitiveType() {
      return TimeTZ;
    }

    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        Instant inst = (Instant) val;

        long micros = inst.getMicrosLocal() % DAYS.toMicros(1);

        int tzOffsetSecs = (int) -inst.getZoneOffsetSecs();

        buffer.writeInt(12);
        buffer.writeLong(micros);
        buffer.writeInt(tzOffsetSecs);
      }

    }

  }

}
