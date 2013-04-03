package com.impossibl.postgres.datetime;

import java.util.Map;

import com.impossibl.postgres.datetime.instants.Instant;


public interface DateTimeFormat {
	
	interface Parser {
		
		String YEAR_PIECE 	= "YEAR";
		String MONTH_PIECE 	= "MONTH";
		String DAY_PIECE 		= "DAY";

		String HOUR_PIECE 	= "HOURS";
		String MINUTE_PIECE 	= "MINUTES";
		String SECOND_PIECE 	= "SECONDS";
		String NANOSECOND_PIECE 	= "NANOSECONDS";
		
		String ZONE_PIECE 	= "ZONE";

		String INFINITY_PIECE 	= "INFINITY";

		int parse(String text, int offset, Map<String, Object> pieces);
		
	}
	
	Parser getParser();
	
	
	interface Printer {
	
		String format(Instant instant);
		
	}

	Printer getPrinter();
	
}
