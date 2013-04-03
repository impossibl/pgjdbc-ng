package com.impossibl.postgres.datetime;

import static com.impossibl.postgres.datetime.FormatUtils.checkOffset;
import static com.impossibl.postgres.datetime.FormatUtils.parseInt;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Calendar;
import java.util.Map;

import com.impossibl.postgres.datetime.instants.Instant;



public class ISOTimeFormat implements DateTimeFormat {
	
	Parser parser = new Parser();
	Printer printer = new Printer();

	@Override
	public Parser getParser() {
		return parser;
	}

	@Override
	public Printer getPrinter() {
		return printer;
	}
	
	
	static final String GMT_ID = "GMT";
	
	static class Parser implements DateTimeFormat.Parser {
		
		@Override
		public int parse(String date, int offset, Map<String, Object> pieces) {
	
			try {
	
				int[] parseResult = new int[1];
								
				// extract hours, minutes, seconds and milliseconds
				offset = parseInt(date, offset, parseResult);
				checkOffset(date, offset, ':');
				pieces.put(HOUR_PIECE, parseResult[0]);
	
				offset = parseInt(date, offset + 1, parseResult);
				checkOffset(date, offset, '\0');
				pieces.put(MINUTE_PIECE, parseResult[0]);
	
				//Optional seconds
				if(offset < date.length()) {
					checkOffset(date, offset, ':');
					
					offset = parseInt(date, offset + 1, parseResult);
					checkOffset(date, offset, '\0');
					pieces.put(SECOND_PIECE, parseResult[0]);

					//Optional fraction
					if(offset < date.length()) {
	
						if(date.charAt(offset) == '.') {
							
							checkOffset(date, offset, '.');
							
							int nanosStart = offset+1;
							offset = parseInt(date, nanosStart, parseResult);
							checkOffset(date, offset, '\0');
							
							int nanoDigits = offset - nanosStart;
							if(nanoDigits > 9) {
								return ~nanosStart;
							}
							
							int nanos = parseResult[0] * (int)Math.pow(10, 9 - nanoDigits);
							pieces.put(NANOSECOND_PIECE, nanos);
							
						}
					}
				}
	
				// extract timezone
				if(offset < date.length()) {
					String timeZoneId = null;
					char timeZoneIndicator = date.charAt(offset);
					if(timeZoneIndicator == '+' || timeZoneIndicator == '-') {
						timeZoneId = GMT_ID + date.substring(offset);
					}
					else if(timeZoneIndicator == 'Z') {
						timeZoneId = GMT_ID;
					}
					
					if(timeZoneId != null) {
						pieces.put(ZONE_PIECE, timeZoneId);
					}
				}
				
				return offset;
			}
			catch(IndexOutOfBoundsException | IllegalArgumentException e) {
			}
	
			return offset;
		}

	}
	
	static class Printer implements DateTimeFormat.Printer {

		@Override
		public String format(Instant instant) {
	
			Calendar cal = Calendar.getInstance(instant.getZone());
			cal.setTimeInMillis(MICROSECONDS.toMillis(instant.getMicrosUTC()));
	
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			int day = cal.get(Calendar.DAY_OF_MONTH);
	
			char buf[] = "2000-00-00".toCharArray();
			buf[0] = Character.forDigit(year / 1000, 10);
			buf[1] = Character.forDigit((year / 100) % 10, 10);
			buf[2] = Character.forDigit((year / 10) % 10, 10);
			buf[3] = Character.forDigit(year % 10, 10);
			buf[5] = Character.forDigit(month / 10, 10);
			buf[6] = Character.forDigit(month % 10, 10);
			buf[8] = Character.forDigit(day / 10, 10);
			buf[9] = Character.forDigit(day % 10, 10);
	
			return new String(buf);
		}
		
	}

}
