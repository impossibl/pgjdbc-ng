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
package com.impossibl.postgres.utils;

import com.impossibl.postgres.system.ConversionException;

import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public class TimestampParts {

  private boolean hasDate = false;
  private int era = GregorianCalendar.AD;
  private int year = 1970;
  private int month = 1;

  private boolean hasTime = false;
  private int day = 1;
  private int hour = 0;
  private int minute = 0;
  private int second = 0;
  private int nanos = 0;

  private TimeZone tz = null;

  public int getEra() {
    return era;
  }

  public int getYear() {
    return year;
  }

  public int getMonth() {
    return month;
  }

  public int getDay() {
    return day;
  }

  public int getHour() {
    return hour;
  }

  public int getMinute() {
    return minute;
  }

  public int getSecond() {
    return second;
  }

  public int getNanos() {
    return nanos;
  }

  public TimeZone getTz() {
    return tz;
  }

  public static TimestampParts parse(String str) throws ConversionException {
    char[] s = str.toCharArray();
    int slen = s.length;

    // This is pretty gross..
    TimestampParts result = new TimestampParts();

    // We try to parse these fields in order; all are optional
    // (but some combinations don't make sense, e.g. if you have
    // both date and time then they must be whitespace-separated).
    // At least one of date and time must be present.

    // leading whitespace
    // yyyy-mm-dd
    // whitespace
    // hh:mm:ss
    // whitespace
    // timezone in one of the formats: +hh, -hh, +hh:mm, -hh:mm
    // whitespace
    // if date is present, an era specifier: AD or BC
    // trailing whitespace

    try {
      int start = skipWhitespace(s, 0); // Skip leading whitespace
      int end = firstNonDigit(s, start);
      int num;
      char sep;

      // Possibly read date.
      if (charAt(s, end) == '-') {
        //
        // Date
        //
        result.hasDate = true;

        // year
        result.year = number(s, start, end);
        start = end + 1; // Skip '-'

        // month
        end = firstNonDigit(s, start);
        result.month = number(s, start, end);

        sep = charAt(s, end);
        if (sep != '-') {
          throw new NumberFormatException("Expected date to be dash-separated, got '" + sep + "'");
        }

        start = end + 1; // Skip '-'

        // day of month
        end = firstNonDigit(s, start);
        result.day = number(s, start, end);

        start = skipWhitespace(s, end); // Skip trailing whitespace
      }

      // Possibly read time.
      if (Character.isDigit(charAt(s, start))) {
        //
        // Time.
        //

        result.hasTime = true;

        // Hours

        end = firstNonDigit(s, start);
        result.hour = number(s, start, end);

        sep = charAt(s, end);
        if (sep != ':') {
          throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");
        }

        start = end + 1; // Skip ':'

        // minutes

        end = firstNonDigit(s, start);
        result.minute = number(s, start, end);

        sep = charAt(s, end);
        if (sep != ':') {
          throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");
        }

        start = end + 1; // Skip ':'

        // seconds

        end = firstNonDigit(s, start);
        result.second = number(s, start, end);
        start = end;

        // Fractional seconds.
        if (charAt(s, start) == '.') {
          end = firstNonDigit(s, start + 1); // Skip '.'
          num = number(s, start + 1, end);

          for (int numlength = (end - (start + 1)); numlength < 9; ++numlength) {
            num *= 10;
          }

          result.nanos = num;
          start = end;
        }

        start = skipWhitespace(s, start); // Skip trailing whitespace
      }

      // Possibly read timezone.
      sep = charAt(s, start);
      if (sep == '-' || sep == '+') {
        int tzsign = (sep == '-') ? -1 : 1;
        int tzhr;
        int tzmin;
        int tzsec;

        end = firstNonDigit(s, start + 1); // Skip +/-
        tzhr = number(s, start + 1, end);
        start = end;

        sep = charAt(s, start);
        if (sep == ':') {
          end = firstNonDigit(s, start + 1); // Skip ':'
          tzmin = number(s, start + 1, end);
          start = end;
        }
        else {
          tzmin = 0;
        }

        tzsec = 0;
        sep = charAt(s, start);
        if (sep == ':') {
          end = firstNonDigit(s, start + 1); // Skip ':'
          tzsec = number(s, start + 1, end);
          start = end;
        }

        // Setting offset does not seem to work correctly in all
        // cases.. So get a fresh calendar for a synthetic timezone
        // instead
        result.tz = getTimeZone(tzsign, tzhr, tzmin, tzsec);

        start = skipWhitespace(s, start); // Skip trailing whitespace
      }

      if (result.hasDate && start < slen) {
        String eraString = new String(s, start, slen - start);
        if (eraString.startsWith("AD")) {
          result.era = GregorianCalendar.AD;
          start += 2;
        }
        else if (eraString.startsWith("BC")) {
          result.era = GregorianCalendar.BC;
          start += 2;
        }
      }

      if (start < slen) {
        throw new NumberFormatException("Trailing junk on timestamp: '" + new String(s, start, slen - start) + "'");
      }

      if (!result.hasTime && !result.hasDate) {
        throw new NumberFormatException("Timestamp has neither date nor time");
      }

    }
    catch (NumberFormatException nfe) {
      throw new ConversionException("Bad value for type timestamp/date/time");
    }

    return result;
  }

  private static int skipWhitespace(char[] s, int start) {
    int slen = s.length;
    for (int i = start; i < slen; i++) {
      if (!Character.isWhitespace(s[i])) {
        return i;
      }
    }
    return slen;
  }

  private static int firstNonDigit(char[] s, int start) {
    int slen = s.length;
    for (int i = start; i < slen; i++) {
      if (!Character.isDigit(s[i])) {
        return i;
      }
    }
    return slen;
  }

  private static int number(char[] s, int start, int end) {
    if (start >= end) {
      throw new NumberFormatException();
    }
    int n = 0;
    for (int i = start; i < end; i++) {
      n = 10 * n + (s[i] - '0');
    }
    return n;
  }

  private static char charAt(char[] s, int pos) {
    if (pos >= 0 && pos < s.length) {
      return s[pos];
    }
    return '\0';
  }

  private static TimeZone getTimeZone(int sign, int hr, int min, int sec) {
    int rawOffset = sign * (((hr * 60 + min) * 60 + sec) * 1000);

    StringBuilder zoneID = new StringBuilder("GMT");
    zoneID.append(sign < 0 ? '-' : '+');
    if (hr < 10) {
      zoneID.append('0');
    }
    zoneID.append(hr);
    if (min < 10) {
      zoneID.append('0');
    }
    zoneID.append(min);
    if (sec < 10) {
      zoneID.append('0');
    }
    zoneID.append(sec);

    return new SimpleTimeZone(rawOffset, zoneID.toString());
  }

}
