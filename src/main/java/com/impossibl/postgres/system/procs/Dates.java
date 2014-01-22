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

import com.impossibl.postgres.datetime.instants.AmbiguousInstant;
import com.impossibl.postgres.datetime.instants.FutureInfiniteInstant;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.Instants;
import com.impossibl.postgres.datetime.instants.PastInfiniteInstant;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.PrimitiveType.Date;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;

public class Dates extends SimpleProcProvider {

  public Dates() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "date_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Date;
    }

    @Override
    public Class<?> getOutputType() {
      return Instant.class;
    }

    @Override
    public Instant decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 4) {
        throw new IOException("invalid length");
      }

      int daysPg = buffer.readInt();

      if (daysPg == Integer.MAX_VALUE) {
        return FutureInfiniteInstant.INSTANCE;
      }
      else if (daysPg == Integer.MIN_VALUE) {
        return PastInfiniteInstant.INSTANCE;
      }

      long micros = toJavaMicros(daysPg);

      return new AmbiguousInstant(Instant.Type.Date, micros);
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Instant.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Date;
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {
      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        Instant inst = (Instant) val;

        int daysPg;

        if (inst.getType() == Instant.Type.Infinity) {
          daysPg = inst.getMicrosLocal() < 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }
        else {
          daysPg = toPgDays(inst);
        }

        buffer.writeInt(4);
        buffer.writeInt(daysPg);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {
      return val == null ? 4 : 8;
    }

  }

  private static final long PG_EPOCH_SECS = 946684800L;

  static final long DAY_SECS = DAYS.toSeconds(1);

  static final long CUTOFF_1_START_SECS = -13165977600L;  // October 15, 1582 -> October 4, 1582
  static final long CUTOFF_1_END_SECS   = -12219292800L;
  static final long CUTOFF_2_START_SECS = -15773356800L;  // 1500-03-01 -> 1500-02-28
  static final long CUTOFF_2_END_SECS   = -14825808000L;
  static final long APPROX_YEAR_SECS1   = -3155823050L;
  static final long APPROX_YEAR_SECS2   =  3155760000L;

  private static int toPgDays(Instant a) {

    long secs = MICROSECONDS.toSeconds(a.getMicrosLocal());

    secs -= PG_EPOCH_SECS;

    // Julian/Greagorian calendar cutoff point

    if (secs < CUTOFF_1_START_SECS) {
      secs -= DAY_SECS * 10;
      if (secs < CUTOFF_2_START_SECS) {
        int years = (int) ((secs - CUTOFF_2_START_SECS) / APPROX_YEAR_SECS1);
        years++;
        years -= years / 4;
        secs += years * DAY_SECS;
      }
    }

    return (int) Math.floor((double)secs / (double)DAY_SECS);
  }

  private static long toJavaMicros(long days) {

    long secs = DAYS.toSeconds(days);

    secs += PG_EPOCH_SECS;

    // Julian/Gregorian calendar cutoff point

    if (secs < CUTOFF_1_END_SECS) {
      secs += DAY_SECS * 10;
      if (secs < CUTOFF_2_END_SECS) {
        int extraLeaps = (int) ((secs - CUTOFF_2_END_SECS) / APPROX_YEAR_SECS2);
        extraLeaps--;
        extraLeaps -= extraLeaps / 4;
        secs += extraLeaps * DAY_SECS;
      }
    }

    return SECONDS.toMicros(secs);
  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    public Class<?> getOutputType() {
      return Instant.class;
    }

    @Override
    Object decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      Map<String, Object> pieces = new HashMap<>();

      context.getDateFormatter().getParser().parse(buffer.toString(), 0, pieces);

      return Instants.dateFromPieces(pieces, context.getTimeZone()).ambiguate();
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    public Class<?> getInputType() {
      return Instant.class;
    }

    @Override
    void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      String strVal = context.getDateFormatter().getPrinter().format((Instant) val);

      buffer.append(strVal);
    }

  }

}
