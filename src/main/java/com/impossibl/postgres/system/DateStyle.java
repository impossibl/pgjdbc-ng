package com.impossibl.postgres.system;

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.datetime.ISODateFormat;
import com.impossibl.postgres.datetime.ISOTimeFormat;
import com.impossibl.postgres.datetime.ISOTimestampFormat;

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
	public static DateTimeFormat getDateFormatter(String[] dateStyle) {
		
		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
		default:
			//Currently everything is ISO
			return new ISODateFormat();
		}
		
	}

	/**
	 * Creates a DateFormat for handling Times from a parsed DateStyle string
	 * 
	 * @param dateStyle Parsed DateStyle
	 * @returns DateFormat for handling times in the style specified in dateStyle
	 */
	public static DateTimeFormat getTimeFormatter(String[] dateStyle) {
		
		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
		default:
			//Currently everything is ISO
			return new ISOTimeFormat();
		}

	}

	/**
	 * Creates a DateFormat for handling Timestamps from a parsed DateStyle string
	 * 
	 * @param dateStyle Parsed DateStyle
	 * @returns DateFormat for handling timestamps in the style specified in dateStyle
	 */
	public static DateTimeFormat getTimestampFormatter(String[] dateStyle) {
		
		switch(dateStyle[0]) {
		case "ISO":
		case "POSTGRES":			
		case "SQL":
		case "GERMAN":
		default:
			//Currently everything is ISO
			return new ISOTimestampFormat();
		}
		
	}

}
