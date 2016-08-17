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

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


class DatesTimes {

  static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  static final String POS_INFINITY = "infinity";
  static final String NEG_INFINITY = "-infinity";

  static final long JAVA_DATE_POSITIVE_INFINITY_MSECS = 9223372036825200000L;
  static final long JAVA_DATE_NEGATIVE_INFINITY_MSECS = -9223372036832400000L;

  private static final long PG_EPOCH_SECS = 946684800L;

  private static final long DAY_SECS = DAYS.toSeconds(1);
  private static final long DAY_MSECS = DAYS.toMillis(1);

  private static final long CUTOFF_1_START_SECS = -13165977600L;  // October 15, 1582 -> October 4, 1582
  private static final long CUTOFF_1_END_SECS = -12219292800L;
  private static final long CUTOFF_2_START_SECS = -15773356800L;  // 1500-03-01 -> 1500-02-28
  private static final long CUTOFF_2_END_SECS = -14825808000L;
  private static final long APPROX_YEAR_SECS1 = -3155823050L;
  private static final long APPROX_YEAR_SECS2 = 3155760000L;

  static long timePgToJava(long value, TimeUnit timeUnit) {

    value += timeUnit.convert(PG_EPOCH_SECS, SECONDS);

    // Julian/Gregorian calendar cutoff point

    if (value < timeUnit.convert(CUTOFF_1_END_SECS, SECONDS)) {
      value += timeUnit.convert(DAY_SECS, SECONDS) * 10;
      if (value < timeUnit.convert(CUTOFF_2_END_SECS, SECONDS)) {
        int extraLeaps = (int) ((value - timeUnit.convert(CUTOFF_2_END_SECS, SECONDS)) / timeUnit.convert(APPROX_YEAR_SECS2, SECONDS));
        extraLeaps--;
        extraLeaps -= extraLeaps / 4;
        value += extraLeaps * timeUnit.convert(DAY_SECS, SECONDS);
      }
    }

    return value;
  }

  static long timeJavaToPg(long value, TimeUnit timeUnit) {

    value -= timeUnit.convert(PG_EPOCH_SECS, SECONDS);

    // Julian/Gregorian calendar cutoff point

    if (value < timeUnit.convert(CUTOFF_1_START_SECS, SECONDS)) {
      value -= timeUnit.convert(DAY_SECS, SECONDS) * 10;
      if (value < timeUnit.convert(CUTOFF_2_START_SECS, SECONDS)) {
        int years = (int) ((value - timeUnit.convert(CUTOFF_2_START_SECS, SECONDS)) / timeUnit.convert(APPROX_YEAR_SECS1, SECONDS));
        years++;
        years -= years / 4;
        value += years * timeUnit.convert(DAY_SECS, SECONDS);
      }
    }

    return value;
  }

  private static ThreadLocal<GregorianCalendar> currentCachedCalendar = ThreadLocal.withInitial(GregorianCalendar::new);

  static Calendar getCalendar(TimeZone timeZone) {
    Calendar cal = currentCachedCalendar.get();
    cal.setTimeZone(timeZone);
    return cal;
  }

  /**
   * Converts a timestamp to an SQL date in a timezone
   */
  static long toDateInTimeZone(long millis, TimeZone timeZone) {

    timeZone = timeZone != null ? timeZone : UTC;

    // no adjustments for the inifity hack values
    if (millis <= JAVA_DATE_NEGATIVE_INFINITY_MSECS || millis >= JAVA_DATE_POSITIVE_INFINITY_MSECS) {
      return millis;
    }

    // If there are no DST shifts... simple arithmetic works
    if (isSimpleTimeZone(timeZone)) {

      int offset = timeZone.getRawOffset();

      // 1) Move to UTC
      millis += offset;

      // 2) Truncate hours, minutes, etc.
      millis = millis / DAY_MSECS * DAY_MSECS;

      // 3) Move back to correct timezone
      millis -= offset;

      return millis;
    }

    Calendar cal = getCalendar(timeZone);
    cal.setTimeInMillis(millis);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    return cal.getTimeInMillis();
  }

  /**
   * Converts a timestamp to an SQL time in a timezone; always at date 1970-01-01 in given timezone.
   */
  static long toTimeInTimeZone(long millis, TimeZone timeZone) {

    timeZone = timeZone != null ? timeZone : UTC;

    // If there are no DST shifts... simple arithmetic works
    if (isSimpleTimeZone(timeZone)) {

      int offset = timeZone.getRawOffset();

      // 1) Move to UTC
      millis += offset;

      // 2) Truncate year, month & day
      millis %= DAY_MSECS;

      // 3) Move back to correct timezone
      millis -= offset;

      return millis;
    }

    Calendar cal = getCalendar(timeZone);
    cal.setTimeInMillis(millis);
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(Calendar.YEAR, 1970);
    cal.set(Calendar.MONTH, 0);
    cal.set(Calendar.DAY_OF_MONTH, 1);

    return cal.getTimeInMillis();
  }

  /**
   * Converts a UTC timestamp to a timestamp in a timezone.
   */
  static long toTimestampInTimeZone(long millis, TimeZone timeZone) {

    // Default timezone is UTC... no conversion needed
    if (timeZone == null) {
      return millis;
    }

    // If well known and no shifting... simple subtraction works
    if (isSimpleTimeZone(timeZone)) {
      return millis - timeZone.getRawOffset();
    }

    /**
     * Convert timestamp manually using a gregorian calendar
     * NOTE: Using {@link Calendar#setTimeZone} with a time
     * already set does not work because {@link Calendar} will
     * adjust the hour not the timestamp itself.
     */

    // Setup calendar with UTC timestamp
    Calendar calendar = getCalendar(UTC);
    calendar.setTimeInMillis(millis);

    // Pull out individual pieces
    int era = calendar.get(Calendar.ERA);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int min = calendar.get(Calendar.MINUTE);
    int sec = calendar.get(Calendar.SECOND);
    int ms = calendar.get(Calendar.MILLISECOND);

    // Assemble new timestamp from pieces
    calendar.setTimeZone(timeZone);
    calendar.set(Calendar.ERA, era);
    calendar.set(Calendar.YEAR, year);
    calendar.set(Calendar.MONTH, month);
    calendar.set(Calendar.DAY_OF_MONTH, day);
    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, min);
    calendar.set(Calendar.SECOND, sec);
    calendar.set(Calendar.MILLISECOND, ms);

    return calendar.getTimeInMillis();
  }


  /**
   * Converts a timestamp in give timezone to a utc timestamp.
   */
  static long fromTimestampInTimeZone(long millis, TimeZone timeZone) {

    // Default timezone is UTC... no conversion needed
    if (timeZone == null) {
      return millis;
    }

    // If well known and no shifting... simple subtraction works
    if (isSimpleTimeZone(timeZone)) {
      return millis + timeZone.getRawOffset();
    }

    /**
     * Convert timestamp manually using a gregorian calendar
     * NOTE: Using {@link Calendar#setTimeZone} with a time
     * already set does not work because {@link Calendar} will
     * adjust the hour not the timestamp itself.
     */

    // Setup calendar with UTC timestamp
    Calendar calendar = getCalendar(timeZone);
    calendar.setTimeInMillis(millis);

    // Pull out individual pieces
    int era = calendar.get(Calendar.ERA);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int min = calendar.get(Calendar.MINUTE);
    int sec = calendar.get(Calendar.SECOND);
    int ms = calendar.get(Calendar.MILLISECOND);

    // Assemble new timestamp from pieces
    calendar.setTimeZone(UTC);
    calendar.set(Calendar.ERA, era);
    calendar.set(Calendar.YEAR, year);
    calendar.set(Calendar.MONTH, month);
    calendar.set(Calendar.DAY_OF_MONTH, day);
    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, min);
    calendar.set(Calendar.SECOND, sec);
    calendar.set(Calendar.MILLISECOND, ms);

    return calendar.getTimeInMillis();
  }


  private static boolean isSimpleTimeZone(TimeZone zone) {
    String id = zone.getID();
    return id.startsWith("GMT") || id.startsWith("UTC");
  }

  static long timeFromParsed(TemporalAccessor parsed, TimeZone zone) {

    int hours = getChrono(parsed, ChronoField.HOUR_OF_DAY, 0);
    int minutes = getChrono(parsed, ChronoField.MINUTE_OF_HOUR, 0);
    int seconds = getChrono(parsed, ChronoField.SECOND_OF_MINUTE, 0);
    int nanoseconds = getChrono(parsed, ChronoField.NANO_OF_SECOND, 0);

    TimeZone inZone = null;
    if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(parsed.get(ChronoField.OFFSET_SECONDS));
      inZone = TimeZone.getTimeZone(offset.normalized());
    }

    long micros = 0;
    micros += HOURS.toMicros(hours);
    micros += MINUTES.toMicros(minutes);
    micros += SECONDS.toMicros(seconds);
    micros += NANOSECONDS.toMicros(nanoseconds);

    if (inZone != null) {
      //Convert to instant
      micros -= MILLISECONDS.toMicros(inZone.getOffset(MICROSECONDS.toMillis(micros)));
    }

    //Convert time to zone local time
    if (zone != null) {
      micros -= MILLISECONDS.toMicros(zone.getOffset(MICROSECONDS.toMillis(micros)));
    }

    return micros;
  }

  static long dateFromParsed(TemporalAccessor parsed, TimeZone zone) {

    int year = getChrono(parsed, ChronoField.YEAR, 1970);
    int month = getChrono(parsed, ChronoField.MONTH_OF_YEAR, 1);
    int day = getChrono(parsed, ChronoField.DAY_OF_MONTH, 1);

    if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(parsed.get(ChronoField.OFFSET_SECONDS));
      zone = TimeZone.getTimeZone(offset.normalized());
    }

    Calendar cal = Calendar.getInstance(zone);
    cal.clear();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month - 1);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    return cal.getTimeInMillis();
  }

  private static int getChrono(TemporalAccessor from, ChronoField field, int fieldDefault) {
    if (!from.isSupported(field)) return fieldDefault;
    return from.get(field);
  }

  static long timestampFromParsed(TemporalAccessor parsed, TimeZone zone) {

    int year = getChrono(parsed, ChronoField.YEAR, 1970);
    int month = getChrono(parsed, ChronoField.MONTH_OF_YEAR, 1);
    int day = getChrono(parsed, ChronoField.DAY_OF_MONTH, 1);
    int hours = getChrono(parsed, ChronoField.HOUR_OF_DAY, 0);
    int minutes = getChrono(parsed, ChronoField.MINUTE_OF_HOUR, 0);
    int seconds = getChrono(parsed, ChronoField.SECOND_OF_MINUTE, 0);
    int nanoseconds = getChrono(parsed, ChronoField.NANO_OF_SECOND, 0);

    if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(parsed.get(ChronoField.OFFSET_SECONDS));
      zone = TimeZone.getTimeZone(offset.normalized());
    }

    //Use calendar to calculate date-time down to the second
    Calendar cal = Calendar.getInstance(zone);
    cal.clear();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month - 1);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, hours);
    cal.set(Calendar.MINUTE, minutes);
    cal.set(Calendar.SECOND, seconds);
    cal.set(Calendar.MILLISECOND, 0);

    long millis = cal.getTimeInMillis();
    long micros = MILLISECONDS.toMicros(millis);

    //Add in missing part of nanoseconds
    micros += NANOSECONDS.toMicros(nanoseconds);

    return micros;
  }

}
