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
import static com.impossibl.postgres.system.procs.DatesTimes.UTC_ID;
import static com.impossibl.postgres.system.procs.DatesTimes.javaEpochToPg;
import static com.impossibl.postgres.system.procs.DatesTimes.pgEpochToJava;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;

public class TimestampsWithTZ extends SettingSelectProcProvider {

  public TimestampsWithTZ() {
    super(ServerInfo::hasIntegerDateTimes,
        new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        new TxtEncoder(), new TxtDecoder(), null, null,
        "timestamptz_");
  }

  private static OffsetDateTime convertInput(Context context, Type type, Object value, Calendar sourceCalendar) throws ConversionException {

    if (value instanceof OffsetDateTime) {
      return (OffsetDateTime) value;
    }

    if (value instanceof CharSequence) {
      CharSequence chars = (CharSequence) value;

      if (value.equals(POS_INFINITY)) return OffsetDateTime.MAX;
      if (value.equals(NEG_INFINITY)) return OffsetDateTime.MIN;

      TemporalAccessor parsed = context.getClientTimestampFormat().getParser().parse(chars);

      if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
        return OffsetDateTime.from(parsed);
      }

      return LocalDateTime.from(parsed).atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime();
    }
    else if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      if (ts.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetDateTime.MAX;
      if (ts.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetDateTime.MIN;

      return ts.toInstant().atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime();
    }
    else if (value instanceof Time) {
      Time t = (Time) value;
      if (t.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetDateTime.MAX;
      if (t.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetDateTime.MIN;

      return Instant.ofEpochMilli(t.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime();
    }
    else if (value instanceof Date) {
      Date d = (Date) value;
      if (d.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return OffsetDateTime.MAX;
      if (d.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return OffsetDateTime.MIN;

      return Instant.ofEpochMilli(d.getTime()).atZone(sourceCalendar.getTimeZone().toZoneId()).toOffsetDateTime();
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertInfinityOutput(boolean positive, Type type, Class<?> targetClass) throws ConversionException {

    if (targetClass == OffsetDateTime.class) {
      return positive ? OffsetDateTime.MAX : OffsetDateTime.MIN;
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

  private static Object convertOutput(Context context, Type type, ZonedDateTime dateTime, Class<?> targetClass, Calendar targetCalendar) throws ConversionException {

    if (targetClass == OffsetDateTime.class) {
      return dateTime.toOffsetDateTime();
    }

    if (targetClass == String.class) {
      return context.getClientTimestampFormat().getPrinter().format(dateTime);
    }

    ZoneId targetZoneId = targetCalendar.getTimeZone().toZoneId();
    ZonedDateTime zonedDateTime = dateTime.withZoneSameInstant(targetZoneId);

    if (targetClass == Time.class) {
      return new Time(zonedDateTime.withYear(1970).withDayOfYear(1).toInstant().toEpochMilli());
    }

    if (targetClass == Date.class) {
      return new Date(zonedDateTime.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
    }

    if (targetClass == Timestamp.class) {
      return Timestamp.from(zonedDateTime.toInstant());
    }

    throw new ConversionException(type, targetClass);
  }

  private static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(8);
    }

    @Override
    public Class<?> getDefaultClass() {
      return Timestamp.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros = buffer.readLong();

      if (micros == Long.MAX_VALUE || micros == Long.MIN_VALUE) {
        return convertInfinityOutput(micros == Long.MAX_VALUE, type, targetClass);
      }

      micros = pgEpochToJava(micros, MICROSECONDS);

      long secs = MICROSECONDS.toSeconds(micros);
      long nanos = MICROSECONDS.toNanos(micros % SECONDS.toMicros(1));
      if (nanos < 0) {
        nanos += SECONDS.toNanos(1);
        secs--;
      }

      ZonedDateTime dateTime = Instant.ofEpochSecond(secs, (int) nanos).atZone(context.getTimeZoneId());

      return convertOutput(context, type, dateTime, targetClass, calendar);
    }

  }

  private static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(8);
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      OffsetDateTime dateTime = convertInput(context, type, value, calendar);

      long micros;
      if (dateTime.equals(OffsetDateTime.MAX)) {
        micros = Long.MAX_VALUE;
      }
      else if (dateTime.equals(OffsetDateTime.MIN)) {
        micros = Long.MIN_VALUE;
      }
      else {

        long seconds = javaEpochToPg(dateTime.toEpochSecond(), SECONDS);
        // Convert to micros rounding nanoseconds
        micros = SECONDS.toMicros(seconds) + NANOSECONDS.toMicros(dateTime.getNano() + 500);
      }

      buffer.writeLong(micros);
    }

  }

  private static class TxtDecoder extends BaseTextDecoder {

    public Class<?> getDefaultClass() {
      return Timestamp.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      if (buffer.equals(POS_INFINITY) || buffer.equals(NEG_INFINITY)) {
        return convertInfinityOutput(buffer.equals(POS_INFINITY), type, targetClass);
      }

      TemporalAccessor parsed = context.getServerTimestampFormat().getParser().parse(buffer);

      ZonedDateTime dateTime;
      if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
        dateTime = OffsetDateTime.from(parsed).toZonedDateTime().withZoneSameInstant(UTC_ID);
      }
      else {
        dateTime = LocalDateTime.from(parsed).atZone(calendar.getTimeZone().toZoneId());
      }

      return convertOutput(context, type, dateTime, targetClass, calendar);
    }

  }

  private static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      OffsetDateTime dateTime = convertInput(context, type, value, calendar);
      if (dateTime.equals(OffsetDateTime.MAX)) {
        buffer.append(POS_INFINITY);
      }
      else if (dateTime.equals(OffsetDateTime.MIN)) {
        buffer.append(NEG_INFINITY);
      }
      else {

        String strVal = context.getServerTimestampFormat().getPrinter().format(dateTime);

        buffer.append(strVal);
      }
    }

  }

}
