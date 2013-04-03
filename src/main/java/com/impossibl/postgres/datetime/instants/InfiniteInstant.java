package com.impossibl.postgres.datetime.instants;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import com.impossibl.postgres.datetime.TimeZones;


public abstract class InfiniteInstant implements Instant {

	@Override
	public Type getType() {
		return Type.Infinity;
	}

	@Override
	public Date toDate() {
		return new Date(getMillisLocal());
	}

	@Override
	public Time toTime() {
		throw new IllegalStateException("Infinity cannot be represented as a 'time' value");
	}

	@Override
	public Timestamp toTimestamp() {

		long micros = getMicrosLocal();
		long millis = MICROSECONDS.toMillis(micros);
		long leftoverMicros = micros - MILLISECONDS.toMicros(millis);
		
		Timestamp ts = new Timestamp(millis);
		
		long nanos = ts.getNanos() + MICROSECONDS.toNanos(leftoverMicros);
		ts.setNanos((int) nanos);
		
		return ts;
	}

	@Override
	public TimeZone getZone() {
		return TimeZones.UTC;
	}

	@Override
	public long getZoneOffsetSecs() {
		return 0;
	}

	@Override
	public long getZoneOffsetMicros() {
		return 0;
	}

	@Override
	public long getZoneOffsetMillis() {
		return 0;
	}

	@Override
	public Instant switchTo(TimeZone zone) {
		return this;
	}

	@Override
	public Instant disambiguate(TimeZone zone) {
		return this;
	}

	@Override
	public Instant ambiguate() {
		return this;
	}

	@Override
	public Instant add(int field, int value) {
		return this;
	}

	@Override
	public Instant subtract(int field, int value) {
		return this;
	}

}
