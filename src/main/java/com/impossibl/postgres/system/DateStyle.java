package com.impossibl.postgres.system;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Utility methods for handling PostgreSQL DateStyle parameters
 * 
 * @author kdubb
 *
 */
public class DateStyle {

	/**
	 * Parses a DateStyle string into its separate components
	 * 
	 * @param value DateStyle to parse
	 * @return Parsed DateStyle components
	 */
	public static String[] parse(String value) {
		
		String[] parsed = value.split(",");
		if(parsed.length != 2)
			return null;
		
		parsed[0] = parsed[0].trim().toUpperCase();
		parsed[1] = parsed[1].trim().toUpperCase();
		
		return parsed;
	}
	
	/**
	 * Creates a DateFormat for handling Dates from a parsed DateStyle string
	 * 
	 * @param dateStyle Parsed DateStyle
	 * @returns DateFormat for handling dates in the style specified in dateStyle
	 */
	public static DateTimeFormatter getDateFormatter(String[] dateStyle) {
		
		//TODO do we actually need to do this or is Java formats acceptable
		//given that we transmit in binary 

		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
			DateTimeParser[] parsers = {
					ISODateTimeFormat.dateParser().getParser(),
					DateTimeFormat.shortDate().getParser(),
					DateTimeFormat.mediumDate().getParser(),
					DateTimeFormat.longDate().getParser(),
					DateTimeFormat.fullDate().getParser()
			};
			
			DateTimePrinter printer = DateTimeFormat.fullDate().getPrinter();

			return new DateTimeFormatterBuilder().append(printer, parsers).toFormatter();
		}
		
		return null;
	}

	/**
	 * Creates a DateFormat for handling Times from a parsed DateStyle string
	 * 
	 * @param dateStyle Parsed DateStyle
	 * @returns DateFormat for handling times in the style specified in dateStyle
	 */
	public static DateTimeFormatter getTimeFormatter(String[] dateStyle) {
		
		//TODO do we actually need to do this or is Java formats acceptable
		//given that we transmit in binary 

		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
			DateTimeParser[] parsers = {
					ISODateTimeFormat.timeParser().getParser(),
					DateTimeFormat.shortTime().getParser(),
					DateTimeFormat.mediumTime().getParser(),
					DateTimeFormat.longTime().getParser(),
					DateTimeFormat.fullTime().getParser()
			};
			
			DateTimePrinter printer = DateTimeFormat.fullTime().getPrinter();

			return new DateTimeFormatterBuilder().append(printer, parsers).toFormatter();
		}
		
		return null;
	}

	/**
	 * Creates a DateFormat for handling Timestamps from a parsed DateStyle string
	 * 
	 * @param dateStyle Parsed DateStyle
	 * @returns DateFormat for handling timestamps in the style specified in dateStyle
	 */
	public static DateTimeFormatter getTimestampFormatter(String[] dateStyle) {
		
		//TODO do we actually need to do this or is Java formats acceptable
		//given that we transmit in binary 

		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
			DateTimeParser[] parsers = {
					ISODateTimeFormat.dateTimeParser().getParser(),
					DateTimeFormat.shortDateTime().getParser(),
					DateTimeFormat.mediumDateTime().getParser(),
					DateTimeFormat.longDateTime().getParser(),
					DateTimeFormat.fullDateTime().getParser()
			};
			
			DateTimePrinter printer = DateTimeFormat.fullTime().getPrinter();

			return new DateTimeFormatterBuilder().append(printer, parsers).toFormatter();
		}
		
		return null;
	}

}
