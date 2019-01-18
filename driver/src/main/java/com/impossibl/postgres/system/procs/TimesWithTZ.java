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
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
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

public class TimesWithTZ extends SettingSelectProcProvider {

  public TimesWithTZ() {
    super(ServerInfo::hasIntegerDateTimes,
        new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        new TxtEncoder(), new TxtDecoder(), null, null,
        "timetz_");
  }

  private static OffsetTime convertInput(Context context, Type type, Object value, Calendar sourceCalendar) throws ConversionException {

    if (value instanceof OffsetTime) {
      return (OffsetTime) value;
    }

    if (value instanceof LocalTime) {
      if (value == LocalTime.MAX) return OffsetTime.MAX;
      if (value == LocalTime.MIN) return OffsetTime.MIN;
      LocalTime localTime = (LocalTime) value;
      ZoneOffset offset =
          ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(sourceCalendar.getTimeZone().getRawOffset()));
      return localTime.atOffset(offset);
    }

    if (value instanceof CharSequence) {
      CharSequence chars = (CharSequence) value;

      if (value.equals(POS_INFINITY)) return OffsetTime.MAX;
      if (value.equals(NEG_INFINITY)) return OffsetTime.MIN;

      TemporalAccessor parsed = context.getTimeFormat().getParser().parse(chars);

      if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
        return OffsetTime.from(parsed);
      }

      ZoneOffset offset =
          ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(sourceCalendar.getTimeZone().getRawOffset()));
      return LocalTime.from(parsed).atOffset(offset);
    }

    if (value instanceof Time) {
      Time t = (Time) value;
      if (t.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetTime.MAX;
      if (t.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetTime.MIN;

      return Instant.ofEpochMilli(t.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime().toOffsetTime();
    }

    if (value instanceof Date) {
      java.sql.Date d = (java.sql.Date) value;
      if (d.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetTime.MAX;
      if (d.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetTime.MIN;

      return Instant.ofEpochMilli(d.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime().toOffsetTime();
    }

    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      if (ts.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetTime.MAX;
      if (ts.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetTime.MIN;

      return ts.toInstant().atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime().toOffsetTime();
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertInfinityOutput(boolean positive, Type type, Class<?> targetClass) throws ConversionException {

    if (targetClass == OffsetTime.class) {
      return positive ? OffsetTime.MAX : OffsetTime.MIN;
    }

    if (targetClass == String.class) {
      return positive ? POS_INFINITY : NEG_INFINITY;
    }

    if (targetClass == Time.class) {
      return new Time(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    if (targetClass == Timestamp.class) {
      return new Timestamp(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    throw new ConversionException(type, targetClass);
  }

  private static Object convertOutput(Context context, Type type, OffsetTime time, Class<?> targetClass, Calendar targetCalendar) throws ConversionException {

    if (targetClass == OffsetTime.class) {
      return time;
    }

    if (targetClass == String.class) {
      return context.getTimeFormat().getPrinter().format(time);
    }

    if (targetClass == Time.class) {
      targetCalendar.clear();
      LocalDate date = LocalDate.of(1970, 1, 1);
      ZonedDateTime dateTime = time.atDate(date).atZoneSameInstant(targetCalendar.getTimeZone().toZoneId());
      return new Time(dateTime.toInstant().toEpochMilli());
    }

    if (targetClass == Timestamp.class) {
      LocalDate date = LocalDate.of(1970, 1, 1);
      ZonedDateTime dateTime = date.atTime(time).atZoneSameInstant(targetCalendar.getTimeZone().toZoneId());
      return Timestamp.from(dateTime.toInstant());
    }

    throw new ConversionException(type, targetClass);
  }


  private static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(12);
    }

    @Override
    public Class<?> getDefaultClass() {
      return Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros = buffer.readLong();
      int tzOffsetSecs = -buffer.readInt();

      if (micros == Long.MAX_VALUE || micros == Long.MIN_VALUE) {
        return convertInfinityOutput(micros == Long.MAX_VALUE, type, targetClass);
      }

      ZoneOffset offset = ZoneOffset.ofTotalSeconds(tzOffsetSecs);
      OffsetTime time = LocalTime.ofNanoOfDay(MICROSECONDS.toNanos(micros)).atOffset(offset);

      return convertOutput(context, type, time, targetClass, calendar);
    }

  }

  private static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(12);
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      OffsetTime time = convertInput(context, type, value, calendar);

      int tzOffsetSecs = -time.getOffset().getTotalSeconds();
      long micros;
      if (time.equals(OffsetTime.MAX)) {
        micros = Long.MAX_VALUE;
      }
      else if (time.equals(OffsetTime.MIN)) {
        micros = Long.MIN_VALUE;
      }
      else {
        // Convert to micros rounding nanoseconds
        micros = NANOSECONDS.toMicros(time.toLocalTime().toNanoOfDay() + 500) % DAYS.toMicros(1);
      }

      buffer.writeLong(micros);
      buffer.writeInt(tzOffsetSecs);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      if (buffer.equals(POS_INFINITY) || buffer.equals(NEG_INFINITY)) {
        return convertInfinityOutput(buffer.equals(POS_INFINITY), type, targetClass);
      }

      TemporalAccessor parsed = context.getTimeFormat().getParser().parse(buffer);

      OffsetTime time;
      if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
        time = OffsetTime.from(parsed);
      }
      else {
        ZoneOffset offset =
            ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(calendar.getTimeZone().getRawOffset()));
        time = LocalTime.from(parsed).atOffset(offset);
      }

      return convertOutput(context, type, time, targetClass, calendar);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      OffsetTime time = convertInput(context, type, value, calendar);

      if (time.equals(OffsetTime.MAX)) {
        buffer.append(POS_INFINITY);
      }
      else if (time.equals(OffsetTime.MIN)) {
        buffer.append(NEG_INFINITY);
      }
      else {

        String strVal = context.getTimeFormat().getPrinter().format(time);

        buffer.append(strVal);
      }

    }

  }

}
