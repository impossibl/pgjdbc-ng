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

import com.impossibl.postgres.datetime.instants.FutureInfiniteInstant;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.PastInfiniteInstant;

import static com.impossibl.postgres.datetime.FormatUtils.checkOffset;
import static com.impossibl.postgres.datetime.FormatUtils.parseInt;

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class ISODateFormat implements DateTimeFormat {

  Parser parser = new Parser();
  Printer printer = new Printer();

  @Override
  public Parser getParser() {
    return parser;
  }

  @Override
  public Printer getPrinter() {
    return printer;
  }


  static class Parser implements DateTimeFormat.Parser {

    @Override
    public int parse(String date, int offset, Map<String, Object> pieces) {

      try {

        if (date.equals("infinity")) {
          pieces.put(INFINITY_PIECE, FutureInfiniteInstant.INSTANCE);
          offset = date.length();
        }
        else if (date.equals("-infinity")) {
          pieces.put(INFINITY_PIECE, PastInfiniteInstant.INSTANCE);
          offset = date.length();
        }
        else {

          int[] parsedValue = new int[1];

          // extract year
          offset = parseInt(date, offset, parsedValue);
          checkOffset(date, offset, '-');
          pieces.put(YEAR_PIECE, parsedValue[0]);

          // extract month
          offset = parseInt(date, offset + 1, parsedValue);
          checkOffset(date, offset, '-');
          pieces.put(MONTH_PIECE, parsedValue[0]);

          // extract day
          offset = parseInt(date, offset += 1, parsedValue);
          checkOffset(date, offset, '\0');
          pieces.put(DAY_PIECE, parsedValue[0]);

        }

      }
      catch (IndexOutOfBoundsException | IllegalArgumentException e) {
        // Ignore
      }

      return offset;
    }

  }


  static class Printer implements DateTimeFormat.Printer {

    @Override
    public String format(Instant instant) {

      TimeZone zone = instant.getZone();
      if (zone == null) {
        zone = TimeZone.getTimeZone("UTC");
      }

      Calendar cal = Calendar.getInstance(zone);
      cal.setTimeInMillis(MICROSECONDS.toMillis(instant.getMicrosUTC()));

      int year = cal.get(Calendar.YEAR);
      int month = cal.get(Calendar.MONTH) + 1;
      int day = cal.get(Calendar.DAY_OF_MONTH);

      String yearString;
      String monthString;
      String dayString;
      String yearZeros = "0000";

      if (year < 1000) {
        // Add leading zeros
        yearString = "" + year;
        yearString = yearZeros.substring(0, (4 - yearString.length())) + yearString;
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

      // do a string builder here instead.
      StringBuilder timestampBuf = new StringBuilder(20);
      timestampBuf.append(yearString);
      timestampBuf.append("-");
      timestampBuf.append(monthString);
      timestampBuf.append("-");
      timestampBuf.append(dayString);
      timestampBuf.append(" ");

      return (timestampBuf.toString());
    }

  }

}
