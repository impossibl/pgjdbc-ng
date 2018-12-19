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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Misc utils for handling time and date values.
 */
public class TimestampUtils {

  private final Calendar defaultCal = new GregorianCalendar();

  static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
  static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;

  public static Timestamp timestampOf(long micros) {
    return Timestamp.from(Instant.EPOCH.plus(Duration.ofNanos(MICROSECONDS.toNanos(micros))));
  }

  public static Date dateOf(long micros) {
    return Date.valueOf(LocalDate.from(Instant.EPOCH.plus(Duration.ofNanos(MICROSECONDS.toNanos(micros)))));
  }

  public static Time timeOf(long micros) {
    return Time.valueOf(LocalTime.from(Instant.EPOCH.plus(Duration.ofNanos(MICROSECONDS.toNanos(micros)))));
  }

  public String toString(Calendar calParameter, Timestamp x) {
    Calendar cal = getLocalCalendar(calParameter);
    cal.setTime(x);

    StringBuilder sb = new StringBuilder();

    if (x.equals(timestampOf(DATE_POSITIVE_INFINITY))) {
      sb.append("infinity");
    }
    else if (x.equals(timestampOf(DATE_NEGATIVE_INFINITY))) {
      sb.append("-infinity");
    }
    else {
      appendDate(sb, cal);
      sb.append(' ');
      appendTime(sb, cal, x.getNanos());
      appendTimeZone(sb, cal);
      appendEra(sb, cal);
    }

    return sb.toString();
  }

  public String toTimestampString(Calendar cal, Date x) {

    if (x.equals(new Date(DATE_POSITIVE_INFINITY))) {
      return "infinity";
    }
    else if (x.equals(new Date(DATE_NEGATIVE_INFINITY))) {
      return "-infinity";
    }
    else {
      return toString(cal, new Timestamp(x.getTime()));
    }
  }

  public String toString(Calendar calParameter, Date x) {
    Calendar cal = getLocalCalendar(calParameter);

    cal.setTime(x);

    StringBuilder sb = new StringBuilder();

    if (x.equals(new Date(DATE_POSITIVE_INFINITY))) {
      sb.append("infinity");
    }
    else if (x.equals(new Date(DATE_NEGATIVE_INFINITY))) {
      sb.append("-infinity");
    }
    else {
      appendDate(sb, cal);
      appendEra(sb, cal);
      appendTimeZone(sb, cal);
    }

    return sb.toString();
  }

  public String toTimestampString(Calendar cal, Time x) {
    return toString(cal, new Timestamp(x.getTime()));
  }

  public String toString(Calendar calParameter, Time x) {
    Calendar cal = getLocalCalendar(calParameter);

    StringBuilder sb = new StringBuilder();

    appendTime(sb, cal, cal.get(Calendar.MILLISECOND) * 1000000);

    appendTimeZone(sb, cal);

    return sb.toString();
  }

  private static void appendDate(StringBuilder sb, Calendar cal) {
    int l_year = cal.get(Calendar.YEAR);
    // always use at least four digits for the year so very
    // early years, like 2, don't get misinterpreted
    //
    int l_yearlen = String.valueOf(l_year).length();
    for (int i = 4; i > l_yearlen; i--) {
      sb.append("0");
    }

    sb.append(l_year);
    sb.append('-');
    int l_month = cal.get(Calendar.MONTH) + 1;
    if (l_month < 10)
      sb.append('0');
    sb.append(l_month);
    sb.append('-');
    int l_day = cal.get(Calendar.DAY_OF_MONTH);
    if (l_day < 10)
      sb.append('0');
    sb.append(l_day);
  }

  private static void appendTime(StringBuilder sb, Calendar cal, int nanos) {
    int hours = cal.get(Calendar.HOUR_OF_DAY);
    if (hours < 10)
      sb.append('0');
    sb.append(hours);

    sb.append(':');
    int minutes = cal.get(Calendar.MINUTE);
    if (minutes < 10)
      sb.append('0');
    sb.append(minutes);

    sb.append(':');
    int seconds = cal.get(Calendar.SECOND);
    if (seconds < 10)
      sb.append('0');
    sb.append(seconds);

    // Add nanoseconds.
    // This won't work for server versions < 7.2 which only want
    // a two digit fractional second, but we don't need to support 7.1
    // anymore and getting the version number here is difficult.
    //
    char[] decimalStr = {'0', '0', '0', '0', '0', '0', '0', '0', '0'};
    char[] nanoStr = Integer.toString(nanos).toCharArray();
    System.arraycopy(nanoStr, 0, decimalStr, decimalStr.length - nanoStr.length, nanoStr.length);
    sb.append('.');
    sb.append(decimalStr, 0, 6);
  }

  private void appendTimeZone(StringBuilder sb, java.util.Calendar cal) {
    int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / 1000;

    int absoff = Math.abs(offset);
    int hours = absoff / 60 / 60;
    int mins = (absoff - hours * 60 * 60) / 60;
    int secs = absoff - hours * 60 * 60 - mins * 60;

    sb.append((offset >= 0) ? " +" : " -");

    if (hours < 10)
      sb.append('0');
    sb.append(hours);

    sb.append(':');

    if (mins < 10)
      sb.append('0');
    sb.append(mins);

    sb.append(':');
    if (secs < 10)
      sb.append('0');
    sb.append(secs);
  }

  private static void appendEra(StringBuilder sb, Calendar cal) {
    if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
      sb.append(" BC");
    }
  }

  private Calendar getLocalCalendar(Calendar cal) {
    if (cal == null) {
      return (Calendar) defaultCal.clone();
    }
    return (Calendar) cal.clone();
  }

}
