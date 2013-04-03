package com.impossibl.postgres.datetime.instants;

import static com.impossibl.postgres.datetime.TimeZones.UTC;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Calendar;
import java.util.TimeZone;

import com.impossibl.postgres.system.Context;

public class PreciseInstant extends InstantBase {
	
	private TimeZone zone;
	private long micros;
	
	public PreciseInstant(Type type, long micros, TimeZone zone) {
		super(type);
		this.zone = zone;
		this.micros = micros;
	}

	public TimeZone getZone() {
		return zone;
	}
	
	public long getZoneOffsetSecs() {
		return MILLISECONDS.toSeconds(getZoneOffsetMillis());
	}

	public long getZoneOffsetMicros() {
		return MILLISECONDS.toMicros(getZoneOffsetMillis());
	}

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
		return "";
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

}
