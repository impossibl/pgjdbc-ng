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
package com.impossibl.postgres.data;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Interval {

  private int totalMonths;
  private int totalDays;
  private long totalMicros;

  private static final long SECS_TO_MICROS = SECONDS.toMicros(1);
  private static final long SECS_TO_MILLIS = SECONDS.toMillis(1);

  private static final DecimalFormat secondsFormat;
  static {
    secondsFormat = new DecimalFormat("0.00####");
    DecimalFormatSymbols dfs = secondsFormat.getDecimalFormatSymbols();
    dfs.setDecimalSeparator('.');
    secondsFormat.setDecimalFormatSymbols(dfs);
  }


  public Interval(int months, int days, long timeMicros) {
    super();
    setValue(months, days, timeMicros);
  }

  public Interval(int years, int months, int days, int hours, int minutes, double seconds) {
    super();
    setValue(years, months, days, hours, minutes, seconds);
  }

  public Interval(String interval) {
    super();
    setValue(interval);
  }

  public Interval() {
  }

  public void setValue(String value) {

    boolean isISOFormat = !value.startsWith("@");

    // Just a simple '0'
    if (!isISOFormat && value.length() == 3 && value.charAt(2) == '0') {
      totalMonths = totalDays = 0;
      totalMicros = 0;
      return;
    }

    int years = 0;
    int months = 0;
    int days = 0;
    int hours = 0;
    int minutes = 0;
    double seconds = 0;
    boolean ago = false;

    try {

      String valueToken = null;

      value = value.replace('+', ' ').replace('@', ' ');

      StringTokenizer st = new StringTokenizer(value);
      while (st.hasMoreTokens()) {

        String token = st.nextToken();

        if (token.equals("ago")) {
          ago = true;
          break;
        }

        int endHours = token.indexOf(':');
        if (endHours == -1) {
          valueToken = token;
        }
        else {

          // This handles hours, minutes, seconds and microseconds for
          // ISO intervals
          int offset = (token.charAt(0) == '-') ? 1 : 0;

          hours = parseInt(token.substring(offset + 0, endHours));
          minutes = parseInt(token.substring(endHours + 1, endHours + 3));

          // Pre 7.4 servers do not put second information into the results
          // unless it is non-zero.
          int endMinutes = token.indexOf(':', endHours + 1);
          if (endMinutes != -1)
            seconds = parseDouble(token.substring(endMinutes + 1));

          if (offset == 1) {
            hours = -hours;
            minutes = -minutes;
            seconds = -seconds;
          }

          valueToken = null;

          break;
        }

        if (!st.hasMoreTokens()) {
          throw new IllegalArgumentException("invalid interval");
        }

        token = st.nextToken();

        // This handles years, months, days for both, ISO and
        // Non-ISO intervals. Hours, minutes, seconds and microseconds
        // are handled for Non-ISO intervals here.

        if (token.startsWith("year"))
          years = parseInt(valueToken);
        else if (token.startsWith("mon"))
          months = parseInt(valueToken);
        else if (token.startsWith("day"))
          days = parseInt(valueToken);
        else if (token.startsWith("hour"))
          hours = parseInt(valueToken);
        else if (token.startsWith("min"))
          minutes = parseInt(valueToken);
        else if (token.startsWith("sec"))
          seconds = parseDouble(valueToken);

      }
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid interval", e);
    }

    if (!isISOFormat && ago) {
      // Inverse the leading sign
      setValue(-years, -months, -days, -hours, -minutes, -seconds);
    }
    else {
      setValue(years, months, days, hours, minutes, seconds);
    }

  }

  public void setValue(int months, int days, long timeMicros) {
    this.totalMonths = months;
    this.totalDays = days;
    this.totalMicros = timeMicros;
  }

  public void setValue(int years, int months, int days, int hours, int minutes, double seconds) {
    this.totalMonths += years * 12 + months;
    this.totalDays = days;
    this.totalMicros += HOURS.toMicros(hours);
    this.totalMicros += MINUTES.toMicros(minutes);
    this.totalMicros += (long) (seconds * SECONDS.toMicros(1));
  }

  public long getRawTime() {
    return totalMicros;
  }

  public void setRawTime(long time) {
    this.totalMicros = time;
  }

  public int getRawDays() {
    return totalDays;
  }

  public void setRawDays(int days) {
    this.totalDays = days;
  }

  public int getRawMonths() {
    return totalMonths;
  }

  public void setRawMonths(int months) {
    this.totalMonths = months;
  }

  public long getHours() {
    return MICROSECONDS.toHours(totalMicros);
  }

  public void setHours(long hours) {
    totalMicros -= HOURS.toMicros(getHours());
    totalMicros += HOURS.toMicros(hours);
  }

  public long getMinutes() {
    return MICROSECONDS.toMinutes(totalMicros) - HOURS.toMinutes(getHours());
  }

  public void setMinutes(long minutes) {
    totalMicros -= MINUTES.toMicros(getMinutes());
    totalMicros += MINUTES.toMicros(minutes);
  }

  public double getSeconds() {
    double microsDiff = totalMicros - MINUTES.toMicros(MICROSECONDS.toMinutes(totalMicros));
    return microsDiff / SECS_TO_MICROS;
  }

  public void setSeconds(double seconds) {
    totalMicros -= getSeconds() * SECS_TO_MICROS;
    totalMicros += seconds * SECS_TO_MICROS;
  }

  public int getDays() {
    return totalDays;
  }

  public void setDays(int days) {
    this.totalDays = days;
  }

  public int getMonths() {
    return totalMonths % 12;
  }

  public void setMonths(int months) {
    totalMonths -= getMonths();
    totalMonths = months;
  }

  public int getYears() {
    return totalMonths / 12;
  }

  public void setYears(int years) {
    totalMonths -= getYears() * 12;
    totalMonths += years * 12;
  }

  public void addTo(Calendar cal) {

    long millis = round(getSeconds() * SECS_TO_MILLIS);

    cal.add(Calendar.MILLISECOND, (int) millis);
    cal.add(Calendar.MINUTE, (int) getMinutes());
    cal.add(Calendar.HOUR, (int) getHours());
    cal.add(Calendar.DAY_OF_MONTH, getDays());
    cal.add(Calendar.MONTH, getMonths());
    cal.add(Calendar.YEAR, getYears());
  }

  public void addTo(Date date) {

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    addTo(cal);

    date.setTime(cal.getTimeInMillis());
  }

  public void addTo(Interval interval) {
    interval.totalMonths += totalMonths;
    interval.totalDays += totalDays;
    interval.totalMicros += totalMicros;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + totalDays;
    result = prime * result + totalMonths;
    result = prime * result + (int) (totalMicros ^ (totalMicros >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Interval other = (Interval) obj;
    if (totalDays != other.totalDays)
      return false;
    if (totalMonths != other.totalMonths)
      return false;
    if (totalMicros != other.totalMicros)
      return false;
    return true;
  }

  @Override
  public String toString() {

    StringBuilder buffer = new StringBuilder();

    buffer.
      append("@ ").
      append(getYears()).append(" years ").
      append(getMonths()).append(" months ").
      append(getDays()).append(" days ").
      append(getHours()).append(" hours ").
      append(getMinutes()).append(" minutes ").
      append(format("%f", getSeconds())).append(" seconds");

    return buffer.toString();
  }

}
