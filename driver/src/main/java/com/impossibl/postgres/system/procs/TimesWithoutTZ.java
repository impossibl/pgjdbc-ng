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
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_NEGATIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_POSITIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.NEG_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.POS_INFINITY;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.netty.buffer.ByteBuf;

public class TimesWithoutTZ extends SettingSelectProcProvider {


  public TimesWithoutTZ() {
    super(ServerInfo::hasIntegerDateTimes,
        new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        new TxtEncoder(), new TxtDecoder(), null, null,
        "time_");
  }

  private static LocalTime convertInput(Context context, Type type, Object value, Calendar sourceCalendar) throws ConversionException {

    if (value instanceof LocalTime) {
      return (LocalTime) value;
    }

    if (value instanceof CharSequence) {
      CharSequence chars = (CharSequence) value;

      if (value.equals(POS_INFINITY)) return LocalTime.MAX;
      if (value.equals(NEG_INFINITY)) return LocalTime.MIN;

      TemporalAccessor parsed = context.getTimeFormat().getParser().parse(chars);

      ZoneOffset offset =
          ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(sourceCalendar.getTimeZone().getRawOffset()));

      if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
        return OffsetTime.from(parsed).withOffsetSameInstant(offset).toLocalTime();
      }

      return LocalTime.from(parsed).atOffset(offset).toLocalTime();
    }

    if (value instanceof Time) {
      Time t = (Time) value;
      if (t.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return LocalTime.MAX;
      if (t.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return LocalTime.MIN;

      return Instant.ofEpochMilli(t.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toLocalTime();
    }

    if (value instanceof Date) {
      java.sql.Date d = (java.sql.Date) value;
      if (d.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return LocalTime.MAX;
      if (d.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return LocalTime.MIN;

      return Instant.ofEpochMilli(d.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toLocalTime();
    }

    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      if (ts.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return LocalTime.MAX;
      if (ts.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return LocalTime.MIN;

      return ts.toInstant().atZone(sourceCalendar.getTimeZone().toZoneId()).toLocalTime();
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertInfinityOutput(boolean positive, Type type, Class<?> targetClass) throws ConversionException {

    if (targetClass == LocalTime.class) {
      return positive ? LocalTime.MAX : LocalTime.MIN;
    }

    if (targetClass == OffsetTime.class) {
      return positive ? OffsetTime.MAX : OffsetTime.MIN;
    }

    if (targetClass == String.class) {
      return positive ? POS_INFINITY : NEG_INFINITY;
    }

    if (targetClass == Time.class) {
      return new Time(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    if (targetClass == Date.class) {
      return new Date(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    if (targetClass == Timestamp.class) {
      return new Timestamp(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    throw new ConversionException(type, targetClass);
  }

  private static Object convertOutput(Context context, Type type, LocalTime time, Class<?> targetClass, Calendar targetCalendar) throws ConversionException {

    if (targetClass == LocalTime.class) {
      return time;
    }

    if (targetClass == OffsetTime.class) {
      ZoneOffset offset =
          ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(targetCalendar.getTimeZone().getRawOffset()));
      return time.atOffset(offset);
    }

    if (targetClass == String.class) {
      return context.getTimeFormat().getPrinter().format(time);
    }

    if (targetClass == Time.class) {
      LocalDate date = LocalDate.of(1970, 1, 1);
      OffsetDateTime dateTime = date.atTime(time).atZone(targetCalendar.getTimeZone().toZoneId()).toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC);
      return new Time(dateTime.toInstant().toEpochMilli());
    }

    if (targetClass == Timestamp.class) {
      LocalDate date = LocalDate.of(1970, 1, 1);
      ZonedDateTime dateTime = date.atTime(time).atZone(targetCalendar.getTimeZone().toZoneId());
      return Timestamp.from(dateTime.toInstant());
    }

    throw new ConversionException(type, targetClass);
  }

  private static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(8);
    }

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros = buffer.readLong();

      if (micros == Long.MAX_VALUE || micros == Long.MIN_VALUE) {
        return convertInfinityOutput(micros == Long.MAX_VALUE, type, targetClass);
      }

      LocalTime time = LocalTime.ofNanoOfDay(MICROSECONDS.toNanos(micros));

      return convertOutput(context, type, time, targetClass, calendar);
    }

  }

  private static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(8);
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      LocalTime time = convertInput(context, type, value, calendar);

      long micros;
      if (time.equals(LocalTime.MAX)) {
        micros = Long.MAX_VALUE;
      }
      else if (time.equals(LocalTime.MIN)) {
        micros = 0L;
      }
      else {
        // Convert to micros rounding nanoseconds
        micros = NANOSECONDS.toMicros(time.toNanoOfDay() + 500) % DAYS.toMicros(1);
      }

      buffer.writeLong(micros);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      TemporalAccessor parsed = context.getTimeFormat().getParser().parse(buffer);

      LocalTime time = LocalTime.from(parsed);

      return convertOutput(context, type, time, targetClass, calendar);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      LocalTime time = convertInput(context, type, value, calendar);

      if (time.equals(LocalTime.MAX)) {
        buffer.append(POS_INFINITY);
      }
      else if (time.equals(LocalTime.MIN)) {
        buffer.append(NEG_INFINITY);
      }
      else {

        String strVal = context.getTimeFormat().getPrinter().format(time);

        buffer.append(strVal);
      }

    }

  }

}
