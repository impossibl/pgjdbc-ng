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

import com.google.common.base.Strings;
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
