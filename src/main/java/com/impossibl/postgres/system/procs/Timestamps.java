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
import com.impossibl.postgres.datetime.instants.AmbiguousInstant;
import com.impossibl.postgres.datetime.instants.FutureInfiniteInstant;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.Instants;
import com.impossibl.postgres.datetime.instants.PastInfiniteInstant;
import com.impossibl.postgres.datetime.instants.PreciseInstant;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.TimestampTZ;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.jboss.netty.buffer.ChannelBuffer;

public class Timestamps extends SettingSelectProcProvider {

  private static long PG_JAVA_EPOCH_DIFF_MICROS = calculateEpochDifferenceMicros();

  private TimeZone zone;
  private PrimitiveType primitiveType;

  public Timestamps(PrimitiveType primitiveType, String... baseNames) {
    super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
        null, null, null, null,
        null, null, null, null,
        baseNames);
    this.primitiveType = primitiveType;
    this.zone = primitiveType == TimestampTZ ? TimeZones.UTC : null;
    this.matchedBinEncoder = new BinIntegerEncoder();
    this.matchedBinDecoder = new BinIntegerDecoder();
    this.matchedTxtEncoder = this.unmatchedTxtEncoder = new TxtEncoder();
    this.matchedTxtDecoder = this.unmatchedTxtDecoder = new TxtDecoder();
  }

  class BinIntegerDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return primitiveType;
    }

    @Override
    public Class<?> getOutputType() {
      return Instant.class;
    }

    @Override
    public Instant decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 8) {
        throw new IOException("invalid length");
      }

      long micros = buffer.readLong();

      if (micros == Long.MAX_VALUE) {
        return FutureInfiniteInstant.INSTANCE;
      }
      else if (micros == Long.MIN_VALUE) {
        return PastInfiniteInstant.INSTANCE;
      }

      micros += PG_JAVA_EPOCH_DIFF_MICROS;

      if (zone != null)
        return new PreciseInstant(Instant.Type.Timestamp, micros, zone);
      else
        return new AmbiguousInstant(Instant.Type.Timestamp, micros);
    }

  }

  class BinIntegerEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Instant.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return primitiveType;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        Instant inst = (Instant) val;
        val.toString();

        long micros;
        if (primitiveType == PrimitiveType.TimestampTZ) {
          micros = inst.getMicrosUTC();
        }
        else {
          micros = inst.disambiguate(TimeZone.getDefault()).getMicrosLocal();
        }

        if (!isInfinity(micros)) {

          micros -= PG_JAVA_EPOCH_DIFF_MICROS;
        }

        buffer.writeInt(8);
        buffer.writeLong(micros);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {
      return val == null ? 4 : 12;
    }

  }

  private static long calculateEpochDifferenceMicros() {

    Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    pgEpochInJava.clear();
    pgEpochInJava.set(2000, 0, 1);

    return MILLISECONDS.toMicros(pgEpochInJava.getTimeInMillis());
  }

  public static boolean isInfinity(long micros) {

    return micros == Long.MAX_VALUE || micros == Long.MIN_VALUE;
  }

  class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return primitiveType;
    }

    @Override
    public Class<?> getOutputType() {
      return Instant.class;
    }

    @Override
    Object decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      Map<String, Object> pieces = new HashMap<>();

      context.getTimestampFormatter().getParser().parse(buffer.toString(), 0, pieces);

      Instant instant = Instants.timestampFromPieces(pieces, context.getTimeZone());

      if (primitiveType != PrimitiveType.TimestampTZ) {
        instant = instant.ambiguate();
      }

      return instant;
    }

  }

  class TxtEncoder extends TextEncoder {

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return primitiveType;
    }

    @Override
    public Class<?> getInputType() {
      return Instant.class;
    }

    @Override
    void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      String strVal = context.getTimestampFormatter().getPrinter().format((Instant) val);

      buffer.append(strVal);
    }

  }

}
