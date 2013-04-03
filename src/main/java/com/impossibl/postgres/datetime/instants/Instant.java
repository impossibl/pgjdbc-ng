package com.impossibl.postgres.datetime.instants;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import com.impossibl.postgres.system.Context;


public interface Instant {
	
	enum Type {
		Time,
		Date,
		Timestamp,
		Infinity
	}
	
	Type getType();
	
	long getMillisLocal();	
	long getMicrosLocal();	
	
	long getMillisUTC();
	long getMicrosUTC();

	public TimeZone getZone();
	public long getZoneOffsetSecs();
	public long getZoneOffsetMicros();
	public long getZoneOffsetMillis();
	
	Instant switchTo(TimeZone zone);
	Instant disambiguate(TimeZone zone);
	Instant ambiguate();
	
	Date toDate();
	Time toTime();
	Timestamp toTimestamp();

	String print(Context context);

	Instant add(int field, int value);
	Instant subtract(int field, int value);
	
}
