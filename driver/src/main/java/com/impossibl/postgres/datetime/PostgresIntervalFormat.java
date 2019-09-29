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
package com.impossibl.postgres.datetime;

import com.impossibl.postgres.api.data.Interval;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Period;
import java.util.Locale;
import java.util.StringTokenizer;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

public class PostgresIntervalFormat implements IntervalFormat {

  private Interval parse(CharSequence chars) {

    String value = chars.toString();

    boolean isVerbose = value.startsWith("@");

    // Just a simple '0'
    if (isVerbose && value.length() == 3 && value.charAt(2) == '0') {
      return Interval.of(Duration.ofSeconds(0));
    }

    int years = 0;
    int months = 0;
    int days = 0;
    int hours = 0;
    int minutes = 0;
    double seconds = 0;
    boolean ago = false;

    try {

      String valueToken;

      final String changedValue = value.replace('+', ' ').replace('@', ' ');

      StringTokenizer st = new StringTokenizer(changedValue);
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
          if (endMinutes != -1) {
            NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ROOT);
            seconds = numberFormat.parse(token.substring(endMinutes + 1)).doubleValue();
          }

          if (offset == 1) {
            hours = -hours;
            minutes = -minutes;
            seconds = -seconds;
          }

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
    catch (ParseException e) {
      throw new IllegalArgumentException("invalid interval", e);
    }

    if (isVerbose && ago) {
      // Inverse the leading sign
      years = -years;
      months = -months;
      days = -days;
      hours = -hours;
      minutes = -minutes;
      seconds = -seconds;
    }

    Period period = Period.of(years, months, days);
    Duration duration = Duration.ofHours(hours).plusMinutes(minutes).plusNanos((long) (seconds * 1_000_000_000));

    return Interval.of(period, duration);
  }

  private DecimalFormat secondsFormat = new DecimalFormat("0.00####", DecimalFormatSymbols.getInstance(Locale.ROOT));

  private String print(Interval interval) {

    StringBuilder buffer = new StringBuilder();

    Period period = interval.getPeriod();
    Duration duration = interval.getDuration();

    long hours = duration.toHours();
    long minutes = duration.minusHours(duration.toHours()).toMinutes();
    long seconds = duration.minusMinutes(duration.toMinutes()).getSeconds();
    double fullSeconds = (double) seconds + ((double) duration.getNano() / (double) 1_000_000_000);

    return buffer
        .append("@ ")
        .append(period.getYears()).append(" years ")
        .append(period.getMonths()).append(" months ")
        .append(period.getDays()).append(" days ")
        .append(hours).append(" hours ")
        .append(minutes).append(" minutes ")
        .append(secondsFormat.format(fullSeconds)).append(" seconds")
        .toString();
  }

  @Override
  public Parser getParser() {
    return this::parse;
  }

  @Override
  public Printer getPrinter() {
    return this::print;
  }

}
