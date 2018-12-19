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

import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_NEGATIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_POSITIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.NEG_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.POS_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.dateFromParsed;
import static com.impossibl.postgres.system.procs.DatesTimes.fromTimestampInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.timeJavaToPg;
import static com.impossibl.postgres.system.procs.DatesTimes.timePgToJava;
import static com.impossibl.postgres.system.procs.DatesTimes.toTimestampInTimeZone;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.buffer.ByteBuf;

public class Dates extends SimpleProcProvider {

  public Dates() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "date_");
  }

  private static long convertInput(Object object, Calendar calendar) throws ConversionException {

    if (object instanceof java.sql.Timestamp) {
      Timestamp ts = (Timestamp) object;

      long millis = ts.getTime();
      if (millis == JAVA_DATE_POSITIVE_INFINITY_MSECS || millis == JAVA_DATE_NEGATIVE_INFINITY_MSECS) {
        return millis;
      }

      calendar.setTimeInMillis(millis);
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      return calendar.getTimeInMillis();
    }

    if (object instanceof java.util.Date) {
      return ((java.util.Date) object).getTime();
    }

    if (object instanceof CharSequence) {
      return Date.valueOf(object.toString()).getTime();
    }

    throw new ConversionException(object.getClass(), PrimitiveType.Date);
  }

  private static Object convertOutput(long millis, Class<?> targetClass, Calendar calendar) throws ConversionException {


    if (targetClass == Timestamp.class) {
      return new Timestamp(millis);
    }

    if (targetClass == String.class) {
      if (millis == JAVA_DATE_POSITIVE_INFINITY_MSECS) {
        return "infinity";
      }
      else if (millis == JAVA_DATE_NEGATIVE_INFINITY_MSECS) {
        return "-infinity";
      }
    }

    Date date = new Date(millis);

    if (targetClass == Date.class) {
      return date;
    }

    if (targetClass == LocalDate.class) {
      return date.toLocalDate();
    }

    if (targetClass == String.class) {
      return date.toString();
    }

    throw new ConversionException(PrimitiveType.Date, targetClass);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(4);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Date.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      int daysPg = buffer.readInt();

      long millis;

      switch (daysPg) {
        case Integer.MAX_VALUE:
          millis = JAVA_DATE_POSITIVE_INFINITY_MSECS;
          break;
        case Integer.MIN_VALUE:
          millis = JAVA_DATE_NEGATIVE_INFINITY_MSECS;
          break;
        default:
          millis = timePgToJava(DAYS.toMillis(daysPg), MILLISECONDS);
          millis = toTimestampInTimeZone(millis, calendar.getTimeZone());
      }

      return convertOutput(millis, targetClass, calendar);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(4);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value, calendar);

      int daysPg;
      if (millis == JAVA_DATE_POSITIVE_INFINITY_MSECS) {
        daysPg = Integer.MAX_VALUE;
      }
      else if (millis == JAVA_DATE_NEGATIVE_INFINITY_MSECS) {
        daysPg = Integer.MIN_VALUE;
      }
      else {
        millis = fromTimestampInTimeZone(millis, calendar.getTimeZone());
        daysPg = (int) MILLISECONDS.toDays(timeJavaToPg(millis, MILLISECONDS));
      }

      buffer.writeInt(daysPg);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Date.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      if (buffer.equals(POS_INFINITY)) {
        return convertOutput(JAVA_DATE_POSITIVE_INFINITY_MSECS, targetClass, calendar);
      }
      else if (buffer.equals(NEG_INFINITY)) {
        return convertOutput(JAVA_DATE_NEGATIVE_INFINITY_MSECS, targetClass, calendar);
      }

      TemporalAccessor parsed = context.getDateFormatter().getParser().parse(buffer);

      return convertOutput(dateFromParsed(parsed, calendar.getTimeZone()), targetClass, calendar);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Date;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long millis = convertInput(value, calendar);
      if (millis == JAVA_DATE_POSITIVE_INFINITY_MSECS) {
        buffer.append(POS_INFINITY);
      }
      else if (millis == JAVA_DATE_NEGATIVE_INFINITY_MSECS) {
        buffer.append(NEG_INFINITY);
      }
      else {

        String strVal = context.getDateFormatter().getPrinter().formatMillis(millis, calendar.getTimeZone(), false);

        buffer.append(strVal);
      }
    }

  }

}
