/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import static java.util.Arrays.asList;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;



/**
 * this class stores supported escaped function
 * 
 * @author Xavier Poinsard
 * @author kdubb
 */
class EscapedFunctions {
	// numeric functions names
	public final static String ABS = "abs";
	public final static String ACOS = "acos";
	public final static String ASIN = "asin";
	public final static String ATAN = "atan";
	public final static String ATAN2 = "atan2";
	public final static String CEILING = "ceiling";
	public final static String COS = "cos";
	public final static String COT = "cot";
	public final static String DEGREES = "degrees";
	public final static String EXP = "exp";
	public final static String FLOOR = "floor";
	public final static String LOG = "log";
	public final static String LOG10 = "log10";
	public final static String MOD = "mod";
	public final static String PI = "pi";
	public final static String POWER = "power";
	public final static String RADIANS = "radians";
	public final static String ROUND = "round";
	public final static String SIGN = "sign";
	public final static String SIN = "sin";
	public final static String SQRT = "sqrt";
	public final static String TAN = "tan";
	public final static String TRUNCATE = "truncate";
	
	public final static List<String> ALL_NUMERIC =
			asList(ABS, ACOS, ASIN, ATAN, ATAN2, CEILING, COS, COT, DEGREES, EXP, FLOOR, LOG, LOG10, MOD, PI, POWER, RADIANS, ROUND, SIGN, SIN, SQRT, TAN, TRUNCATE);

	// string function names
	public final static String ASCII = "ascii";
	public final static String CHAR = "char";
	public final static String CONCAT = "concat";
	public final static String INSERT = "insert"; // change arguments order
	public final static String LCASE = "lcase";
	public final static String LEFT = "left";
	public final static String LENGTH = "length";
	public final static String LOCATE = "locate"; // the 3 args version duplicate
																								// args
	public final static String LTRIM = "ltrim";
	public final static String REPEAT = "repeat";
	public final static String REPLACE = "replace";
	public final static String RIGHT = "right"; // duplicate args
	public final static String RTRIM = "rtrim";
	public final static String SPACE = "space";
	public final static String SUBSTRING = "substring";
	public final static String UCASE = "ucase";
	// soundex is implemented on the server side by
	// the contrib/fuzzystrmatch module. We provide a translation
	// for this in the driver, but since we don't want to bother with run
	// time detection of this module's installation we don't report this
	// method as supported in DatabaseMetaData.
	// difference is currently unsupported entirely.
	
	public final static List<String> ALL_STRING =
			asList(ASCII, CHAR, CONCAT, INSERT, LCASE, LEFT, LENGTH, LOCATE, LTRIM, REPEAT, REPLACE, RIGHT, RTRIM, SPACE, SUBSTRING, UCASE);

	// date time function names
	public final static String CURDATE = "curdate";
	public final static String CURTIME = "curtime";
	public final static String DAYNAME = "dayname";
	public final static String DAYOFMONTH = "dayofmonth";
	public final static String DAYOFWEEK = "dayofweek";
	public final static String DAYOFYEAR = "dayofyear";
	public final static String HOUR = "hour";
	public final static String MINUTE = "minute";
	public final static String MONTH = "month";
	public final static String MONTHNAME = "monthname";
	public final static String NOW = "now";
	public final static String QUARTER = "quarter";
	public final static String SECOND = "second";
	public final static String WEEK = "week";
	public final static String YEAR = "year";
	// for timestampadd and timestampdiff the fractional part of second is not
	// supported
	// by the backend
	// timestampdiff is very partially supported
	public final static String TIMESTAMPADD = "timestampadd";
	public final static String TIMESTAMPDIFF = "timestampdiff";

	public final static List<String> ALL_DATE_TIME =
			asList(CURDATE, CURTIME, DAYNAME, DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, HOUR, MINUTE, MONTH, MONTHNAME, NOW, QUARTER, SECOND, WEEK, YEAR);
			
	// constants for timestampadd and timestampdiff
	public final static String SQL_TSI_ROOT = "SQL_TSI_";
	public final static String SQL_TSI_DAY = "DAY";
	public final static String SQL_TSI_FRAC_SECOND = "FRAC_SECOND";
	public final static String SQL_TSI_HOUR = "HOUR";
	public final static String SQL_TSI_MINUTE = "MINUTE";
	public final static String SQL_TSI_MONTH = "MONTH";
	public final static String SQL_TSI_QUARTER = "QUARTER";
	public final static String SQL_TSI_SECOND = "SECOND";
	public final static String SQL_TSI_WEEK = "WEEK";
	public final static String SQL_TSI_YEAR = "YEAR";

	// system functions
	public final static String DATABASE = "database";
	public final static String IFNULL = "ifnull";
	public final static String USER = "user";

	public final static List<String> ALL_SYSTEM =
			asList(DATABASE, IFNULL, USER);
	
	
	
	/** storage for functions implementations */
	private static Map<String, Method> functionMap = createFunctionMap();

	private static Map<String,Method> createFunctionMap() {
		
		Method[] arrayMeths = EscapedFunctions.class.getDeclaredMethods();
		Map<String,Method> functionMap = new HashMap<>(arrayMeths.length * 2);
		
		for(int i = 0; i < arrayMeths.length; i++) {
			Method meth = arrayMeths[i];
			if(meth.getName().startsWith("sql"))
				functionMap.put(meth.getName().toLowerCase(Locale.US), meth);
		}
		return functionMap;
	}

	/**
	 * get Method object implementing the given function
	 * 
	 * @param functionName
	 *          name of the searched function
	 * @return a Method object or null if not found
	 */
	public static Method getFunction(String functionName) {
		return (Method) functionMap.get("sql" + functionName.toLowerCase(Locale.US));
	}

	// ** numeric functions translations **
	/** ceiling to ceil translation */
	public static String sqlceiling(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("ceil(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ceiling"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** log to ln translation */
	public static String sqllog(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("ln(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** log10 to log translation */
	public static String sqllog10(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("log(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log10"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** power to pow translation */
	public static String sqlpower(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("pow(");
		if(parsedArgs.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "power"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0)).append(',').append(parsedArgs.get(1));
		return buf.append(')').toString();
	}

	/** truncate to trunc translation */
	public static String sqltruncate(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("trunc(");
		if(parsedArgs.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "truncate"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0)).append(',').append(parsedArgs.get(1));
		return buf.append(')').toString();
	}

	// ** string functions translations **
	/** char to chr translation */
	public static String sqlchar(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("chr(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "char"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** concat translation */
	public static String sqlconcat(List<Object> parsedArgs) {
		StringBuffer buf = new StringBuffer();
		buf.append('(');
		for(int iArg = 0; iArg < parsedArgs.size(); iArg++) {
			buf.append(parsedArgs.get(iArg));
			if(iArg != (parsedArgs.size() - 1))
				buf.append(" || ");
		}
		return buf.append(')').toString();
	}

	/** insert to overlay translation */
	public static String sqlinsert(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("overlay(");
		if(parsedArgs.size() != 4) {
			throw new SQLException(GT.tr("{0} function takes four and only four argument.", "insert"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0)).append(" placing ").append(parsedArgs.get(3));
		buf.append(" from ").append(parsedArgs.get(1)).append(" for ").append(parsedArgs.get(2));
		return buf.append(')').toString();
	}

	/** lcase to lower translation */
	public static String sqllcase(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("lower(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "lcase"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** left to substring translation */
	public static String sqlleft(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("substring(");
		if(parsedArgs.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "left"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0)).append(" for ").append(parsedArgs.get(1));
		return buf.append(')').toString();
	}

	/** length translation */
	public static String sqllength(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("length(trim(trailing from ");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "length"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append("))").toString();
	}

	/** locate translation */
	public static String sqllocate(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() == 2) {
			return "position(" + parsedArgs.get(0) + " in " + parsedArgs.get(1) + ")";
		}
		else if(parsedArgs.size() == 3) {
			String tmp = "position(" + parsedArgs.get(0) + " in substring(" + parsedArgs.get(1) + " from " + parsedArgs.get(2) + "))";
			return "(" + parsedArgs.get(2) + "*sign(" + tmp + ")+" + tmp + ")";
		}
		else {
			throw new SQLException(GT.tr("{0} function takes two or three arguments.", "locate"), PSQLState.SYNTAX_ERROR);
		}
	}

	/** ltrim translation */
	public static String sqlltrim(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("trim(leading from ");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ltrim"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** right to substring translation */
	public static String sqlright(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("substring(");
		if(parsedArgs.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "right"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0)).append(" from (length(").append(parsedArgs.get(0)).append(")+1-").append(parsedArgs.get(1));
		return buf.append("))").toString();
	}

	/** rtrim translation */
	public static String sqlrtrim(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("trim(trailing from ");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "rtrim"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** space translation */
	public static String sqlspace(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("repeat(' ',");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "space"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** substring to substr translation */
	public static String sqlsubstring(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() == 2) {
			return "substr(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + ")";
		}
		else if(parsedArgs.size() == 3) {
			return "substr(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + "," + parsedArgs.get(2) + ")";
		}
		else {
			throw new SQLException(GT.tr("{0} function takes two or three arguments.", "substring"), PSQLState.SYNTAX_ERROR);
		}
	}

	/** ucase to upper translation */
	public static String sqlucase(List<Object> parsedArgs) throws SQLException {
		StringBuffer buf = new StringBuffer();
		buf.append("upper(");
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ucase"), PSQLState.SYNTAX_ERROR);
		}
		buf.append(parsedArgs.get(0));
		return buf.append(')').toString();
	}

	/** curdate to current_date translation */
	public static String sqlcurdate(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curdate"), PSQLState.SYNTAX_ERROR);
		}
		return "current_date";
	}

	/** curtime to current_time translation */
	public static String sqlcurtime(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curtime"), PSQLState.SYNTAX_ERROR);
		}
		return "current_time";
	}

	/** dayname translation */
	public static String sqldayname(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayname"), PSQLState.SYNTAX_ERROR);
		}
		return "to_char(" + parsedArgs.get(0) + ",'Day')";
	}

	/** dayofmonth translation */
	public static String sqldayofmonth(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofmonth"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(day from " + parsedArgs.get(0) + ")";
	}

	/**
	 * dayofweek translation adding 1 to postgresql function since we expect
	 * values from 1 to 7
	 */
	public static String sqldayofweek(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofweek"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(dow from " + parsedArgs.get(0) + ")+1";
	}

	/** dayofyear translation */
	public static String sqldayofyear(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofyear"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(doy from " + parsedArgs.get(0) + ")";
	}

	/** hour translation */
	public static String sqlhour(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "hour"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(hour from " + parsedArgs.get(0) + ")";
	}

	/** minute translation */
	public static String sqlminute(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "minute"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(minute from " + parsedArgs.get(0) + ")";
	}

	/** month translation */
	public static String sqlmonth(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "month"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(month from " + parsedArgs.get(0) + ")";
	}

	/** monthname translation */
	public static String sqlmonthname(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "monthname"), PSQLState.SYNTAX_ERROR);
		}
		return "to_char(" + parsedArgs.get(0) + ",'Month')";
	}

	/** quarter translation */
	public static String sqlquarter(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "quarter"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(quarter from " + parsedArgs.get(0) + ")";
	}

	/** second translation */
	public static String sqlsecond(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "second"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(second from " + parsedArgs.get(0) + ")";
	}

	/** week translation */
	public static String sqlweek(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "week"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(week from " + parsedArgs.get(0) + ")";
	}

	/** year translation */
	public static String sqlyear(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "year"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(year from " + parsedArgs.get(0) + ")";
	}

	/** time stamp add */
	public static String sqltimestampadd(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 3) {
			throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampadd"), PSQLState.SYNTAX_ERROR);
		}
		String interval = EscapedFunctions.constantToInterval(parsedArgs.get(0).toString(), parsedArgs.get(1).toString());
		StringBuffer buf = new StringBuffer();
		buf.append("(").append(interval).append("+");
		buf.append(parsedArgs.get(2)).append(")");
		return buf.toString();
	}

	private final static String constantToInterval(String type, String value) throws SQLException {
		if(!type.startsWith(SQL_TSI_ROOT))
			throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
		String shortType = type.substring(SQL_TSI_ROOT.length());
		if(SQL_TSI_DAY.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' day' as interval)";
		else if(SQL_TSI_SECOND.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' second' as interval)";
		else if(SQL_TSI_HOUR.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' hour' as interval)";
		else if(SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' minute' as interval)";
		else if(SQL_TSI_MONTH.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' month' as interval)";
		else if(SQL_TSI_QUARTER.equalsIgnoreCase(shortType))
			return "CAST((" + value + "::int * 3) || ' month' as interval)";
		else if(SQL_TSI_WEEK.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' week' as interval)";
		else if(SQL_TSI_YEAR.equalsIgnoreCase(shortType))
			return "CAST(" + value + " || ' year' as interval)";
		else if(SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType))
			throw new SQLException(GT.tr("Interval {0} not yet implemented", "SQL_TSI_FRAC_SECOND"), PSQLState.SYNTAX_ERROR);
		else
			throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
	}

	/** time stamp diff */
	public static String sqltimestampdiff(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 3) {
			throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampdiff"), PSQLState.SYNTAX_ERROR);
		}
		String datePart = EscapedFunctions.constantToDatePart(parsedArgs.get(0).toString());
		StringBuffer buf = new StringBuffer();
		buf.append("extract( ").append(datePart).append(" from (").append(parsedArgs.get(2)).append("-").append(parsedArgs.get(1)).append("))");
		return buf.toString();
	}

	private final static String constantToDatePart(String type) throws SQLException {
		if(!type.startsWith(SQL_TSI_ROOT))
			throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
		String shortType = type.substring(SQL_TSI_ROOT.length());
		if(SQL_TSI_DAY.equalsIgnoreCase(shortType))
			return "day";
		else if(SQL_TSI_SECOND.equalsIgnoreCase(shortType))
			return "second";
		else if(SQL_TSI_HOUR.equalsIgnoreCase(shortType))
			return "hour";
		else if(SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
			return "minute";
		// See http://archives.postgresql.org/pgsql-jdbc/2006-03/msg00096.php
		/*
		 * else if (SQL_TSI_MONTH.equalsIgnoreCase(shortType)) return "month"; else
		 * if (SQL_TSI_QUARTER.equalsIgnoreCase(shortType)) return "quarter"; else
		 * if (SQL_TSI_WEEK.equalsIgnoreCase(shortType)) return "week"; else if
		 * (SQL_TSI_YEAR.equalsIgnoreCase(shortType)) return "year";
		 */
		else if(SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType))
			throw new SQLException(GT.tr("Interval {0} not yet implemented", "SQL_TSI_FRAC_SECOND"), PSQLState.SYNTAX_ERROR);
		else
			throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
	}

	/** database translation */
	public static String sqldatabase(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "database"), PSQLState.SYNTAX_ERROR);
		}
		return "current_database()";
	}

	/** ifnull translation */
	public static String sqlifnull(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "ifnull"), PSQLState.SYNTAX_ERROR);
		}
		return "coalesce(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + ")";
	}

	/** user translation */
	public static String sqluser(List<Object> parsedArgs) throws SQLException {
		if(parsedArgs.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "user"), PSQLState.SYNTAX_ERROR);
		}
		return "user";
	}
}

/**
 * Used as shim (since file was repurposed from original driver)
 * until a real message translator replaces it
 * 
 * @author kdubb
 *
 */
class GT {
	static String tr(String msg, Object... args) {
		return MessageFormat.format(msg, args);
	}
}

/**
 * Used as shim (since file was repurposed from original driver)
 * until real state constants replace it
 * 
 * @author kdubb
 *
 */
class PSQLState {
	static final String SYNTAX_ERROR = "Syntax Error";
}
