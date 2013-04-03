package com.impossibl.postgres.datetime;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.TimeZone;

public class TimeZones {
	
	public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
	
	public static synchronized TimeZone getOffsetZone(int offsetMillis) {

		return TimeZone.getTimeZone(getOffsetZoneID(offsetMillis));
	}
	
	public static String getOffsetZoneID(int offsetMillis) {
		
		int offsetHours = (int)MILLISECONDS.toHours(offsetMillis);
		int offsetMins = (int)MILLISECONDS.toMinutes(offsetMillis - HOURS.toMillis(offsetHours));
		
		return String.format("GMT%+03d:%02d", offsetHours, offsetMins);
	}

}
