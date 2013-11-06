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

import com.impossibl.postgres.system.Context;

import static com.impossibl.postgres.datetime.TimeZones.UTC;

import java.util.Calendar;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PreciseInstant extends InstantBase {

  private TimeZone zone;
  private long micros;

  public PreciseInstant(Type type, long micros, TimeZone zone) {
    super(type);
    this.zone = zone;
    this.micros = micros;
  }

  @Override
  public TimeZone getZone() {
    return zone;
  }

  @Override
  public long getZoneOffsetSecs() {
    return MILLISECONDS.toSeconds(getZoneOffsetMillis());
  }

  @Override
  public long getZoneOffsetMicros() {
    return MILLISECONDS.toMicros(getZoneOffsetMillis());
  }

  @Override
  public long getZoneOffsetMillis() {
    return zone.getOffset(getMillisLocal());
  }

  @Override
  public long getMillisLocal() {
    return MICROSECONDS.toMillis(micros);
  }

  @Override
  public long getMicrosLocal() {
    return micros;
  }

  @Override
  public long getMillisUTC() {
    return getMillisLocal() - getZoneOffsetMillis();
  }

  @Override
  public long getMicrosUTC() {
    return micros - getZoneOffsetMicros();
  }

  @Override
  public PreciseInstant disambiguate(TimeZone zone) {
    return this;
  }

  @Override
  public PreciseInstant switchTo(TimeZone zone) {
    long zoneOffsetMicros = MILLISECONDS.toMicros(zone.getOffset(getMillisLocal()));
    return new PreciseInstant(type, getMicrosUTC() + zoneOffsetMicros, zone);
  }

  @Override
  public AmbiguousInstant ambiguate() {
    return new AmbiguousInstant(type, micros);
  }

  @Override
  public String print(Context context) {

    switch(type) {
      case Time:
        return toTime().toString();
      case Date:
        return toDate().toString();
      case Timestamp:
        return toTimestamp().toString();
      default:
        return "";
    }

  }

  @Override
  public PreciseInstant add(int field, int amount) {

    long oldMillis = getMillisLocal();

    Calendar cal = Calendar.getInstance(UTC);
    cal.setTimeInMillis(oldMillis);
    cal.add(field, amount);

    long diffMillis = cal.getTimeInMillis() - oldMillis;
    long diffMicros = MILLISECONDS.toMicros(diffMillis);

    return new PreciseInstant(type, micros + diffMicros, zone);
  }

  @Override
  public PreciseInstant subtract(int field, int amount) {

    long oldMillis = getMillisLocal();

    Calendar cal = Calendar.getInstance(UTC);
    cal.setTimeInMillis(oldMillis);
    cal.add(field, amount);

    long diffMillis = cal.getTimeInMillis() - oldMillis;
    long diffMicros = MILLISECONDS.toMicros(diffMillis);

    return new PreciseInstant(type, micros + diffMicros, zone);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (int) (micros ^ (micros >>> 32));
    result = prime * result + ((zone == null) ? 0 : zone.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    PreciseInstant other = (PreciseInstant) obj;
    if (micros != other.micros)
      return false;
    if (zone == null) {
      if (other.zone != null)
        return false;
    }
    else if (zone.getRawOffset() != other.zone.getRawOffset())
      return false;
    return true;
  }

}
