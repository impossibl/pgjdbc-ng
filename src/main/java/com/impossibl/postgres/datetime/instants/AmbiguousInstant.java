package com.impossibl.postgres.datetime.instants;

import static com.impossibl.postgres.datetime.TimeZones.UTC;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Calendar;
import java.util.TimeZone;

import com.impossibl.postgres.system.Context;

public class AmbiguousInstant extends InstantBase {

	long micros;

	public AmbiguousInstant(Type type, long micros) {
		super(type);
		this.micros = micros;
	}

	@Override
	public Instant switchTo(TimeZone zone) {
		return new PreciseInstant(type, micros, zone);
	}

	@Override
	public Instant disambiguate(TimeZone zone) {
		return new PreciseInstant(type, micros, zone);
	}

	@Override
	public AmbiguousInstant ambiguate() {
		return this;
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
		return getMillisLocal();
	}

	@Override
	public long getMicrosUTC() {
		return getMicrosLocal();
	}

	@Override
	public String print(Context context) {
		return "";
	}

	@Override
	public AmbiguousInstant add(int field, int amount) {
		
		long oldMillis = getMillisLocal();
		
		Calendar cal = Calendar.getInstance(UTC);
		cal.setTimeInMillis(oldMillis);
		cal.add(field, amount);
		
		long diffMillis = cal.getTimeInMillis() - oldMillis;
		long diffMicros = MILLISECONDS.toMicros(diffMillis);
		
		return new AmbiguousInstant(type, micros + diffMicros);
	}

	@Override
	public AmbiguousInstant subtract(int field, int amount) {
		
		long oldMillis = getMillisLocal();
		
		Calendar cal = Calendar.getInstance(UTC);
		cal.setTimeInMillis(oldMillis);
		cal.add(field, amount);
		
		long diffMillis = cal.getTimeInMillis() - oldMillis;
		long diffMicros = MILLISECONDS.toMicros(diffMillis);
		
		return new AmbiguousInstant(type, micros + diffMicros);
	}

	@Override
	public TimeZone getZone() {
		return null;
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

}
