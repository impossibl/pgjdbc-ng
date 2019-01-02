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
import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.procs.DatesTimes.fromTimestampInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.timeFromParsed;
import static com.impossibl.postgres.types.PrimitiveType.TimeTZ;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;

public class TimesWithTZ extends SettingSelectProcProvider {

  public TimesWithTZ() {
    super(ServerInfo::hasIntegerDateTimes,
        new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        new TxtEncoder(), new TxtDecoder(), null, null,
        "timetz_");
  }

  private static long convertInput(Object object) throws ConversionException {

    if (object instanceof Time) {
      return ((Time) object).getTime();
    }

    if (object instanceof Timestamp) {
      return ((Timestamp) object).getTime();
    }

    if (object instanceof CharSequence) {
      return Timestamp.valueOf(object.toString()).getTime();
    }

    throw new ConversionException(object.getClass(), PrimitiveType.TimeTZ);
  }

  private static Object convertOutput(Context context, long millis, Class<?> targetClass, TimeZone targetTimeZone) throws ConversionException {

    if (targetClass == String.class) {
      return context.getTimeFormatter().getPrinter().formatMillis(millis, targetTimeZone, true);
    }

    if (targetClass == Timestamp.class) {
      return new Timestamp(millis);
    }

    Time time = new Time(millis);

    if (targetClass == Time.class) {
      return time;
    }

    if (targetClass == LocalTime.class) {
      return time.toLocalTime();
    }

    if (targetClass == Instant.class) {
      return time.toInstant();
    }

    throw new ConversionException(PrimitiveType.TimeTZ, targetClass);
  }


  private static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(12);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return TimeTZ;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      long micros = buffer.readLong();
      int tzOffsetSecs = buffer.readInt();

      int tzOffsetMillis = (int)SECONDS.toMillis(tzOffsetSecs);
      TimeZone timeZone = TimeZone.getTimeZone(ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(-tzOffsetMillis)));

      return convertOutput(context, MICROSECONDS.toMillis(micros) + tzOffsetMillis, targetClass, timeZone);
    }

  }

  private static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(12);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return TimeTZ;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value);

      long utcMillis = fromTimestampInTimeZone(millis, calendar.getTimeZone());

      long micros = MILLISECONDS.toMicros(utcMillis) % DAYS.toMicros(1);
      int tzOffsetSecs = (int) MILLISECONDS.toSeconds(millis - utcMillis);

      buffer.writeLong(micros);
      buffer.writeInt(tzOffsetSecs);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return TimeTZ;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      TemporalAccessor parsed = context.getTimeFormatter().getParser().parse(buffer);

      long micros = timeFromParsed(parsed, null);

      return convertOutput(context, MICROSECONDS.toMillis(micros), targetClass, context.getTimeZone());
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return TimeTZ;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value);

      String strVal = context.getTimeFormatter().getPrinter().formatMillis(millis % DAYS.toMillis(1), calendar.getTimeZone(), true);

      buffer.append(strVal);
    }

  }

}
