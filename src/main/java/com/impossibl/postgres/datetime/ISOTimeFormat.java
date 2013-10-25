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

import static com.impossibl.postgres.datetime.FormatUtils.checkOffset;
import static com.impossibl.postgres.datetime.FormatUtils.parseInt;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Calendar;
import java.util.Map;

import com.impossibl.postgres.datetime.instants.Instant;



public class ISOTimeFormat implements DateTimeFormat {

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


  static final String GMT_ID = "GMT";

  static class Parser implements DateTimeFormat.Parser {

    @Override
    public int parse(String date, int offset, Map<String, Object> pieces) {

      try {

        int[] parseResult = new int[1];

        // extract hours, minutes, seconds and milliseconds
        offset = parseInt(date, offset, parseResult);
        checkOffset(date, offset, ':');
        pieces.put(HOUR_PIECE, parseResult[0]);

        offset = parseInt(date, offset + 1, parseResult);
        checkOffset(date, offset, '\0');
        pieces.put(MINUTE_PIECE, parseResult[0]);

        //Optional seconds
        if(offset < date.length()) {
          checkOffset(date, offset, ':');

          offset = parseInt(date, offset + 1, parseResult);
          checkOffset(date, offset, '\0');
          pieces.put(SECOND_PIECE, parseResult[0]);

          //Optional fraction
          if(offset < date.length()) {

            if(date.charAt(offset) == '.') {

              checkOffset(date, offset, '.');

              int nanosStart = offset+1;
              offset = parseInt(date, nanosStart, parseResult);
              checkOffset(date, offset, '\0');

              int nanoDigits = offset - nanosStart;
              if(nanoDigits > 9) {
                return ~nanosStart;
              }

              int nanos = parseResult[0] * (int)Math.pow(10, 9 - nanoDigits);
              pieces.put(NANOSECOND_PIECE, nanos);

            }
          }
        }

        // extract timezone
        if(offset < date.length()) {
          String timeZoneId = null;
          char timeZoneIndicator = date.charAt(offset);
          if(timeZoneIndicator == '+' || timeZoneIndicator == '-') {
            timeZoneId = GMT_ID + date.substring(offset);
          }
          else if(timeZoneIndicator == 'Z') {
            timeZoneId = GMT_ID;
          }

          if(timeZoneId != null) {
            pieces.put(ZONE_PIECE, timeZoneId);
          }
        }

      }
      catch(IndexOutOfBoundsException | IllegalArgumentException e) {
      }

      return offset;
    }

  }

  static class Printer implements DateTimeFormat.Printer {

    @Override
    public String format(Instant instant) {

      Calendar cal = Calendar.getInstance(instant.getZone());
      cal.setTimeInMillis(MICROSECONDS.toMillis(instant.getMicrosUTC()));

      int year = cal.get(Calendar.YEAR);
      int month = cal.get(Calendar.MONTH) + 1;
      int day = cal.get(Calendar.DAY_OF_MONTH);

      char buf[] = "2000-00-00".toCharArray();
      buf[0] = Character.forDigit(year / 1000, 10);
      buf[1] = Character.forDigit((year / 100) % 10, 10);
      buf[2] = Character.forDigit((year / 10) % 10, 10);
      buf[3] = Character.forDigit(year % 10, 10);
      buf[5] = Character.forDigit(month / 10, 10);
      buf[6] = Character.forDigit(month % 10, 10);
      buf[8] = Character.forDigit(day / 10, 10);
      buf[9] = Character.forDigit(day % 10, 10);

      return new String(buf);
    }

  }

}
