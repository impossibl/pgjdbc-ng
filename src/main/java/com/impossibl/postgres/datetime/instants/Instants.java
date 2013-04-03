package com.impossibl.postgres.datetime.instants;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.datetime.instants.Instant.Type;

public class Instants {
	
	public static Instant timeFromPieces(Map<String, Object> pieces, TimeZone zone) {
		
		Integer hours = (Integer) pieces.get(DateTimeFormat.Parser.HOUR_PIECE);
		if(hours == null) {
			hours = 0;
		}
		
		Integer minutes = (Integer) pieces.get(DateTimeFormat.Parser.MINUTE_PIECE);
		if(minutes == null) {
			minutes = 0;
		}
		
		Integer seconds = (Integer) pieces.get(DateTimeFormat.Parser.SECOND_PIECE);
		if(seconds == null) {
			seconds = 0;
		}
		
		Integer nanoseconds = (Integer) pieces.get(DateTimeFormat.Parser.NANOSECOND_PIECE);
		if(nanoseconds == null) {
			nanoseconds = 0;
		}
		
		TimeZone incZone = null;		
		String zoneId = (String) pieces.get(DateTimeFormat.Parser.ZONE_PIECE);
		if(zoneId != null) {
			incZone = TimeZone.getTimeZone(zoneId);
		}
		
		long micros = 0;
		micros += HOURS.toMicros(hours);
		micros += MINUTES.toMicros(minutes);
		micros += SECONDS.toMicros(seconds);
		micros += NANOSECONDS.toMicros(nanoseconds);
		
		if(incZone != null) {
			//Add in given zone offset
			micros -= MILLISECONDS.toMicros(incZone.getOffset(MICROSECONDS.toMillis(micros)));

			//Convert time to zone local time
			micros += MILLISECONDS.toMicros(zone.getOffset(MICROSECONDS.toMillis(micros)));			
		}
		
		return new PreciseInstant(Type.Time, micros, zone);
		
	}
	
	public static Instant dateFromPieces(Map<String, Object> pieces, TimeZone defaultZone) {
		
		Integer year = (Integer) pieces.get(DateTimeFormat.Parser.YEAR_PIECE);
		if(year == null) {
			year = 1970;
		}
		
		Integer month = (Integer) pieces.get(DateTimeFormat.Parser.MONTH_PIECE);
		if(month == null) {
			month = 1;
		}
		
		Integer day = (Integer) pieces.get(DateTimeFormat.Parser.DAY_PIECE);
		if(day == null) {
			day = 1;
		}
		
		TimeZone zone = defaultZone;		
		String zoneId = (String) pieces.get(DateTimeFormat.Parser.ZONE_PIECE);
		if(zoneId != null) {
			zone = TimeZone.getTimeZone(zoneId);
		}
		
		Calendar cal = Calendar.getInstance(zone);
		cal.clear();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month-1);
		cal.set(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		long millis = cal.getTimeInMillis();
		long micros = MILLISECONDS.toMicros(millis);
		
		//Convert to zone local time
		micros += MILLISECONDS.toMicros(zone.getOffset(millis));

		return new PreciseInstant(Type.Date, micros, zone);
			
	}
	
	public static Instant timestampFromPieces(Map<String, Object> pieces, TimeZone zone) {
		
		Integer year = (Integer) pieces.get(DateTimeFormat.Parser.YEAR_PIECE);
		if(year == null) {
			year = 1970;
		}
		
		Integer month = (Integer) pieces.get(DateTimeFormat.Parser.MONTH_PIECE);
		if(month == null) {
			month = 1;
		}
		
		Integer day = (Integer) pieces.get(DateTimeFormat.Parser.DAY_PIECE);
		if(day == null) {
			day = 1;
		}
		
		Integer hours = (Integer) pieces.get(DateTimeFormat.Parser.HOUR_PIECE);
		if(hours == null) {
			hours = 0;
		}
		
		Integer minutes = (Integer) pieces.get(DateTimeFormat.Parser.MINUTE_PIECE);
		if(minutes == null) {
			minutes = 0;
		}
		
		Integer seconds = (Integer) pieces.get(DateTimeFormat.Parser.SECOND_PIECE);
		if(seconds == null) {
			seconds = 0;
		}
		
		Integer nanoseconds = (Integer) pieces.get(DateTimeFormat.Parser.NANOSECOND_PIECE);
		if(nanoseconds == null) {
			nanoseconds = 0;
		}
		
		String zoneId = (String) pieces.get(DateTimeFormat.Parser.ZONE_PIECE);
		if(zoneId != null) {
			zone = TimeZone.getTimeZone(zoneId);
		}
		
		Object infinity = pieces.get(DateTimeFormat.Parser.INFINITY_PIECE);
		if(infinity != null) {
			return (Instant) infinity;
		}
		else {
		
			//Use calendar to calculate date-time down to the second
			Calendar cal = Calendar.getInstance(zone);
			cal.clear();
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.MONTH, month-1);
			cal.set(Calendar.DAY_OF_MONTH, day);
			cal.set(Calendar.HOUR_OF_DAY, hours);
			cal.set(Calendar.MINUTE, minutes);
			cal.set(Calendar.SECOND, seconds);
			cal.set(Calendar.MILLISECOND, 0);
			
			long millis = cal.getTimeInMillis();
			long micros = MILLISECONDS.toMicros(millis);
			
			//Add in missing part of nanoseconds
			micros += NANOSECONDS.toMicros(nanoseconds);
			
			//Convert to zone local time
			micros += MILLISECONDS.toMicros(zone.getOffset(millis));
			
			return new PreciseInstant(Type.Timestamp, micros, zone);
			
		}
		
	}
	
	public static Instant fromTimestamp(Timestamp ts, TimeZone zone) {
		
		long millis = ts.getTime();
		int extra = (ts.getNanos() % 1000000) / 1000;
		
		if(MILLISECONDS.toNanos(millis) == Long.MIN_VALUE) {
			millis += 1;
			extra -= 1000;
		}		

		long micros = MILLISECONDS.toMicros(millis) + extra;
		if(micros == Long.MAX_VALUE) {
			return FutureInfiniteInstant.INSTANCE;
		}
		else if(micros == Long.MIN_VALUE) {
			return PastInfiniteInstant.INSTANCE;
		}
		
		//Covert to local time
		micros += MILLISECONDS.toMicros(zone.getOffset(millis));
		
		return new PreciseInstant(Type.Timestamp, micros, zone);
	}

	public static Instant fromDate(Date d, TimeZone zone) {
		
		long millis = d.getTime();
		if(millis == Long.MAX_VALUE) {
			return FutureInfiniteInstant.INSTANCE;
		}
		else if(millis == Long.MIN_VALUE) {
			return PastInfiniteInstant.INSTANCE;
		}
		
		//Convert to zone local time
		millis += zone.getOffset(millis);
		
		long micros = MILLISECONDS.toMicros(millis);
		
		return new PreciseInstant(Type.Date, micros, zone);
	}

	public static Instant fromTime(Time t, TimeZone zone) {
		
		long millis = t.getTime();
		long micros = MILLISECONDS.toMicros(millis);

		//Covert to local time
		micros += MILLISECONDS.toMicros(zone.getOffset(millis));
		
		return new PreciseInstant(Type.Time, micros, zone);
	}

}
