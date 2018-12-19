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

import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR_OF_ERA;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class ISODateFormat implements DateTimeFormat {

  private Parser parser = new Parser();
  private Printer printer = new Printer();

  @Override
  public Parser getParser() {
    return parser;
  }

  @Override
  public Printer getPrinter() {
    return printer;
  }


  static class Parser implements DateTimeFormat.Parser {

    private static final DateTimeFormatter PARSER =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR_OF_ERA, 4, 10, SignStyle.NOT_NEGATIVE)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .optionalStart()
            .appendOffset("+HH:mm", "+00")
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .appendPattern("GG")
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT)
            .withChronology(IsoChronology.INSTANCE);

    @Override
    public TemporalAccessor parse(CharSequence date) {
      return PARSER.parse(date);
    }

  }

  static class Printer implements DateTimeFormat.Printer {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Override
    public String formatMicros(long micros, TimeZone timeZone, boolean displayTimeZone) {
      return formatMillis(MICROSECONDS.toMillis(micros), timeZone, displayTimeZone);
    }

    @Override
    public String formatMillis(long millis, TimeZone timeZone, boolean displayTimeZone) {

      if (timeZone == null) {
        timeZone = UTC;
      }

      Calendar cal = Calendar.getInstance(timeZone);
      cal.setTimeInMillis(millis);

      int era = cal.get(Calendar.ERA);
      int year = cal.get(Calendar.YEAR);
      int month = cal.get(Calendar.MONTH) + 1;
      int day = cal.get(Calendar.DAY_OF_MONTH);

      String yearString;
      String monthString;
      String dayString;
      String eraString;
      String yearZeros = "0000";

      if (year < 1000) {
        // Add leading zeros
        yearString = "" + year;
        yearString = yearZeros.substring(0, 4 - yearString.length()) + yearString;
      }
      else {
        yearString = "" + year;
      }
      if (month < 10) {
        monthString = "0" + month;
      }
      else {
        monthString = Integer.toString(month);
      }
      if (day < 10) {
        dayString = "0" + day;
      }
      else {
        dayString = Integer.toString(day);
      }
      if (era < 1) {
        eraString = " BC";
      }
      else {
        eraString = "";
      }

      // do a string builder here instead.

      return yearString + "-" + monthString + "-" + dayString + eraString;
    }

  }

}
