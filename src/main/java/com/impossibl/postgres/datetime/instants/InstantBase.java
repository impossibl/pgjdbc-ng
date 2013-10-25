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
package com.impossibl.postgres.datetime.instants;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import com.impossibl.postgres.utils.guava.Strings;
import com.impossibl.postgres.datetime.TimeZones;
import com.impossibl.postgres.system.Context;

public abstract class InstantBase implements Instant {

  Type type;

  protected InstantBase(Type type) {
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String print(Context context) {
    return toString();
  }

  @Override
  public Date toDate() {

    long day = DAYS.toMillis(1);
    long millis = (getMillisLocal() / day) * day;
    millis -= getZoneOffsetMillis();
    return new Date(millis);
  }

  @Override
  public Time toTime() {

    long millis = getMillisLocal();

    if(type == Type.Timestamp) {
      //Remove "date" portion of timestamp
      millis %= DAYS.toMillis(1);
    }

    millis -= getZoneOffsetMillis();

    return new Time(millis);
  }

  @Override
  public Timestamp toTimestamp() {

    long micros = getMicrosUTC();
    long millis = MICROSECONDS.toMillis(micros);
    long leftoverMicros = micros - MILLISECONDS.toMicros(millis);

    Timestamp ts = new Timestamp(millis);

    long nanos = ts.getNanos() + MICROSECONDS.toNanos(leftoverMicros);
    ts.setNanos((int) nanos);

    return ts;
  }

  @Override
  public String toString() {

    long millis = SECONDS.toMillis(MILLISECONDS.toSeconds(getMillisUTC()));
    int micros = (int) (getMicrosUTC() - MILLISECONDS.toMicros(millis));

    if(micros < 0) {
      millis -= 1000;
      micros += 1000000;
    }

    TimeZone zone = getZone() == null ? TimeZones.UTC : getZone();
    Calendar cal = Calendar.getInstance(zone);
    cal.setTimeInMillis(millis);

    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH) + 1;
    int day = cal.get(Calendar.DAY_OF_MONTH);
    int hour = cal.get(Calendar.HOUR_OF_DAY);
    int minute = cal.get(Calendar.MINUTE);
    int second = cal.get(Calendar.SECOND);

    StringBuilder sb = new StringBuilder(String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second));

    sb.append('.');

    if(micros > 0) {

      String microString = Integer.toString(micros);

      // Add leading zeros
      microString = Strings.padStart(microString, 6, '0');

      // Truncate trailing zeros
      char[] nanosChar = new char[microString.length()];
      microString.getChars(0, microString.length(), nanosChar, 0);
      int truncIndex = 5;
      while (nanosChar[truncIndex] == '0') {
          truncIndex--;
      }

      sb.append(nanosChar, 0, truncIndex + 1);
    }
    else {

      sb.append("000000");
    }

    sb.append(" [");

    if(getZone() != null) {
      sb.append(TimeZones.getOffsetZoneID(zone.getRawOffset()));
    }
    else {
      sb.append("ANY");
    }

    sb.append("]");

    return sb.toString();
  }

}
