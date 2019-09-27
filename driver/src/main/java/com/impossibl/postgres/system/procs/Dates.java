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
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_NEGATIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_POSITIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.NEG_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.POS_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.javaEpochToPg;
import static com.impossibl.postgres.system.procs.DatesTimes.pgEpochToJava;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;

import static java.util.concurrent.TimeUnit.DAYS;

import io.netty.buffer.ByteBuf;

public class Dates extends SimpleProcProvider {

  public Dates() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "date_");
  }

  private static LocalDate convertInput(Context context, Type type, Object value, Calendar sourceCalendar) throws ConversionException {

    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }

    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      if (ts.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return LocalDate.MAX;
      if (ts.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return LocalDate.MIN;
      return ts.toInstant().atZone(sourceCalendar.getTimeZone().toZoneId()).toLocalDate();
    }

    if (value instanceof Date) {
      Date d = (Date) value;
      if (d.getTime() == JAVA_DATE_POSITIVE_INFINITY_MSECS) return LocalDate.MAX;
      if (d.getTime() == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return LocalDate.MIN;
      Calendar calendar = Calendar.getInstance(sourceCalendar.getTimeZone());
      calendar.clear();
      calendar.setTimeInMillis(d.getTime());
      int year = calendar.get(Calendar.ERA) == 0 ? -(calendar.get(Calendar.YEAR) - 1) : calendar.get(Calendar.YEAR);
      return LocalDate.of(year, calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
    }

    if (value instanceof CharSequence) {
      CharSequence s = (CharSequence) value;
      if (s.equals(POS_INFINITY)) return LocalDate.MAX;
      if (s.equals(NEG_INFINITY)) return LocalDate.MIN;

      TemporalAccessor parsed = context.getClientDateFormat().getParser().parse(s);

      return LocalDate.from(parsed);
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertInfinityOutput(boolean positive, Type type, Class<?> targetClass) throws ConversionException {

    if (targetClass == LocalDate.class) {
      return positive ? LocalDate.MAX : LocalDate.MIN;
    }

    if (targetClass == String.class) {
      return positive ? POS_INFINITY : NEG_INFINITY;
    }

    if (targetClass == Date.class) {
      return new Date(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    if (targetClass == Timestamp.class) {
      return new Timestamp(positive ? JAVA_DATE_POSITIVE_INFINITY_MSECS : JAVA_DATE_NEGATIVE_INFINITY_MSECS);
    }

    throw new ConversionException(type, targetClass);
  }

  private static Object convertOutput(Context context, Type type, LocalDate date, Class<?> targetClass, Calendar targetCalendar) throws ConversionException {

    if (targetClass == LocalDate.class) {
      return date;
    }

    if (targetClass == String.class) {
      return context.getClientDateFormat().getPrinter().format(date);
    }

    if (targetClass == Timestamp.class) {
      targetCalendar.clear();
      targetCalendar.set(date.getYear(), date.getMonthValue() - 1, date.getDayOfMonth());
      return new Timestamp(targetCalendar.getTimeInMillis());
    }

    if (targetClass == Date.class) {
      targetCalendar.clear();
      targetCalendar.set(date.getYear(), date.getMonthValue() - 1, date.getDayOfMonth());
      return new Date(targetCalendar.getTimeInMillis());
    }

    throw new ConversionException(type, targetClass);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(4);
    }

    @Override
    public Class<?> getDefaultClass() {
      return Date.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      int daysPg = buffer.readInt();

      if (daysPg == Integer.MAX_VALUE || daysPg == Integer.MIN_VALUE) {
        return convertInfinityOutput(daysPg == Integer.MAX_VALUE, type, targetClass);
      }

      LocalDate date = LocalDate.ofEpochDay(pgEpochToJava(daysPg, DAYS));

      return convertOutput(context, type, date, targetClass, calendar);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(4);
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      LocalDate date = convertInput(context, type, value, calendar);

      if (date == LocalDate.MAX) {
        buffer.writeInt(Integer.MAX_VALUE);
      }
      else if (date == LocalDate.MIN) {
        buffer.writeInt(Integer.MIN_VALUE);
      }
      else {

        int daysPg = (int) javaEpochToPg(date.toEpochDay(), DAYS);

        buffer.writeInt(daysPg);
      }

    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return Date.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      if (buffer.equals(POS_INFINITY) || buffer.equals(NEG_INFINITY)) {
        return convertInfinityOutput(buffer.equals(POS_INFINITY), type, targetClass);
      }

      TemporalAccessor parsed = context.getServerDateFormat().getParser().parse(buffer);

      LocalDate date = LocalDate.from(parsed);

      return convertOutput(context, type, date, targetClass, calendar);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      LocalDate date = convertInput(context, type, value, calendar);

      if (date == LocalDate.MAX) {
        buffer.append(POS_INFINITY);
      }
      else if (date == LocalDate.MIN) {
        buffer.append(NEG_INFINITY);
      }
      else {

        String strVal = context.getServerDateFormat().getPrinter().format(date);

        buffer.append(strVal);
      }

    }

  }

}
