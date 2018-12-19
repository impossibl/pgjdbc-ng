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
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.TimestampParts;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.system.procs.DatesTimes.fromTimestampInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.getCalendar;
import static com.impossibl.postgres.system.procs.DatesTimes.timeFromParsed;
import static com.impossibl.postgres.system.procs.DatesTimes.toTimestampInTimeZone;
import static com.impossibl.postgres.types.PrimitiveType.Time;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.buffer.ByteBuf;

public class TimesWithoutTZ extends SettingSelectProcProvider {


  public TimesWithoutTZ() {
    super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
        new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(),
        new TxtEncoder(), new TxtDecoder(), null, null,
        "time_");
  }


  private static long convertInput(Object object, TimeZone defaultTimeZone) throws ConversionException {

    if (object instanceof java.sql.Time) {
      return ((Time) object).getTime();
    }

    if (object instanceof Timestamp) {
      return ((Timestamp) object).getTime();
    }

    if (object instanceof CharSequence) {
      return parseTime(object.toString(), defaultTimeZone);
    }

    throw new ConversionException(object.getClass(), PrimitiveType.TimeTZ);
  }

  private static Object convertOutput(long millis, Class<?> targetClass) throws ConversionException {

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

    if (targetClass == java.time.Instant.class) {
      return time.toInstant();
    }

    if (targetClass == String.class) {
      return time.toString();
    }

    throw new ConversionException(PrimitiveType.TimeTZ, targetClass);
  }

  private static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(8);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Time;
    }

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros = buffer.readLong();

      long millis = MICROSECONDS.toMillis(micros);

      millis = toTimestampInTimeZone(millis, calendar.getTimeZone());

      return convertOutput(millis, targetClass);
    }

  }

  private static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(8);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Time;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value, calendar.getTimeZone());

      millis = fromTimestampInTimeZone(millis, calendar.getTimeZone());

      long micros = MILLISECONDS.toMicros(millis) % DAYS.toMicros(1);

      buffer.writeLong(micros);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Time;
    }

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Time.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      TemporalAccessor parsed = context.getTimeFormatter().getParser().parse(buffer);

      long micros = timeFromParsed(parsed, calendar.getTimeZone());

      return convertOutput(MICROSECONDS.toMillis(micros), targetClass);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Time;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value, calendar.getTimeZone()) % DAYS.toMillis(1);

      String strVal = context.getTimeFormatter().getPrinter().formatMillis(millis, calendar.getTimeZone(), false);

      buffer.append(strVal);
    }

  }

  private static long parseTime(String value, TimeZone defaultTimeZone) throws ConversionException {

    TimestampParts ts = TimestampParts.parse(value);

    // Translate parsed time into default time zone
    Calendar cal = getCalendar(ts.getTz() != null ? ts.getTz() : defaultTimeZone);
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(Calendar.YEAR, 1970);
    cal.set(Calendar.MONTH, 0);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.HOUR_OF_DAY, ts.getHour());
    cal.set(Calendar.MINUTE, ts.getMinute());
    cal.set(Calendar.SECOND, ts.getSecond());
    cal.set(Calendar.MILLISECOND, ts.getNanos() / 1000000);

    return cal.getTimeInMillis();
  }

}
