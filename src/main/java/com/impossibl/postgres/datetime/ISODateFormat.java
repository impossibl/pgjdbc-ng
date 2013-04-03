package com.impossibl.postgres.datetime;

import static com.impossibl.postgres.datetime.FormatUtils.checkOffset;
import static com.impossibl.postgres.datetime.FormatUtils.parseInt;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Calendar;
import java.util.Map;

import com.impossibl.postgres.datetime.instants.Instant;

public class ISODateFormat implements DateTimeFormat {
	
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
	
		
	static class Parser implements DateTimeFormat.Parser {
		
		@Override
		public int parse(String date, int offset, Map<String, Object> pieces) {
	
			try {
				
				int[] parsedValue = new int[1];
				
				// extract year
				offset = parseInt(date, offset, parsedValue);
				checkOffset(date, offset, '-');
				pieces.put(YEAR_PIECE, parsedValue[0]);
		
				// extract month
				offset = parseInt(date, offset + 1, parsedValue);
				checkOffset(date, offset, '-');
				pieces.put(MONTH_PIECE, parsedValue[0]);
		
				// extract day
				offset = parseInt(date, offset += 1, parsedValue);
				checkOffset(date, offset, '\0');
				pieces.put(DAY_PIECE, parsedValue[0]);
				
				return offset;
			}
			catch(IndexOutOfBoundsException | IllegalArgumentException e) {
			}
			
			return ~offset;
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
	    buf[0] = Character.forDigit(year/1000,10);
	    buf[1] = Character.forDigit((year/100)%10,10);
	    buf[2] = Character.forDigit((year/10)%10,10);
	    buf[3] = Character.forDigit(year%10,10);
	    buf[5] = Character.forDigit(month/10,10);
	    buf[6] = Character.forDigit(month%10,10);
	    buf[8] = Character.forDigit(day/10,10);
	    buf[9] = Character.forDigit(day%10,10);
	
	    return new String(buf);
		}
	  
	}

}
