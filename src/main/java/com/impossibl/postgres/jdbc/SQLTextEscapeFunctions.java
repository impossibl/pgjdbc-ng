/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc;

import static java.util.Arrays.asList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Joiner;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;



/**
 * this class stores supported escaped function
 * 
 * @author Xavier Poinsard
 * @author kdubb
 */
class SQLTextEscapeFunctions {

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
	public final static String RAND = "rand";
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
	public final static String CHAR_LENGTH = "char_length";
	public final static String CHARACTER_LENGTH = "character_length";
	public final static String CONCAT = "concat";
	public final static String INSERT = "insert"; // change arguments order
	public final static String LCASE = "lcase";
	public final static String LEFT = "left";
	public final static String LENGTH = "length";
	public final static String LOCATE = "locate"; // the 3 args version duplicate
																								// args
	public final static String LTRIM = "ltrim";
	public final static String OCTET_LENGTH = "octet_length";
	public final static String POSITION = "position";
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
	public final static String CURRENT_DATE = "current_date";
	public final static String CURTIME = "curtime";
	public final static String CURRENT_TIME = "current_time";
	public final static String CURRENT_TIMESTAMP = "current_timestamp";
	public final static String DAYNAME = "dayname";
	public final static String DAYOFMONTH = "dayofmonth";
	public final static String DAYOFWEEK = "dayofweek";
	public final static String DAYOFYEAR = "dayofyear";
	public final static String EXTRACT = "extract";
	public final static String HOUR = "hour";
	public final static String MINUTE = "minute";
	public final static String MONTH = "month";
	public final static String MONTHNAME = "monthname";
	public final static String NOW = "now";
	public final static String QUARTER = "quarter";
	public final static String SECOND = "second";
	public final static String TIMESTAMPADD = "timestampadd";
	public final static String TIMESTAMPDIFF = "timestampdiff";
	public final static String WEEK = "week";
	public final static String YEAR = "year";

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
		
		Method defaultMeth;
		try {
			defaultMeth = SQLTextEscapeFunctions.class.getDeclaredMethod("defaultEscape", String.class, List.class);
		}
		catch(NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		} 
		
		Map<String,Method> functionMap = new HashMap<>();
		
		//Add defaults for all supported functions
		
		for(String name : ALL_STRING) {
			functionMap.put(name, defaultMeth);
		}
		for(String name : ALL_NUMERIC) {
			functionMap.put(name, defaultMeth);
		}
		for(String name : ALL_DATE_TIME) {
			functionMap.put(name, defaultMeth);
		}
		for(String name : ALL_SYSTEM) {
			functionMap.put(name, defaultMeth);
		}
		
		//Replace default with specialized (if available) 
		
		for(Method meth : SQLTextEscapeFunctions.class.getDeclaredMethods()) {
			
			if(meth.getName().startsWith("sql")) {
				String funcName = meth.getName().substring(3).toLowerCase(Locale.US);
				functionMap.put(funcName, meth);
			}
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
	public static Method getEscapeMethod(String functionName) {
		return (Method) functionMap.get(functionName.toLowerCase(Locale.US));
	}
	
	public static String invokeEscape(Method method, String name, List<Node> args) throws SQLException {
		try {
			return (String) method.invoke(null, name, args);
		}
		catch(InvocationTargetException e) {
			if(e.getCause() instanceof SQLException) {
				throw (SQLException)e.getCause();
			}
			else {
				throw new RuntimeException(e);
			}
		}
		catch(IllegalAccessException | IllegalArgumentException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String defaultEscape(String name, List<Node> args) throws SQLException {		
    // by default the function name is kept unchanged
    StringBuilder sb = new StringBuilder();
    sb.append(name).append('(');
		Joiner.on(",").appendTo(sb, args);
    sb.append(')');
    return sb.toString();    
	}

	// ** numeric functions translations **
	/** rand to random translation */
	public static String sqlrand(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("random(");		
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ceiling"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(")");
		return sb.toString();
	}
	
	/** ceiling to ceil translation */
	public static String sqlceiling(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("ceil(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ceiling"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** log to ln translation */
	public static String sqllog(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("ln(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** log10 to log translation */
	public static String sqllog10(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("log(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log10"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** power to pow translation */
	public static String sqlpower(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("pow(");
		if(args.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "power"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0)).append(',').append(args.get(1));
		return sb.append(')').toString();
	}

	/** truncate to trunc translation */
	public static String sqltruncate(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("trunc(");
		if(args.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "truncate"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0)).append(',').append(args.get(1));
		return sb.append(')').toString();
	}

	// ** string functions translations **
	/** char to chr translation */
	public static String sqlchar(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("chr(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "char"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** concat translation */
	public static String sqlconcat(String name, List<Node> args) {
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		Joiner.on(" || ").appendTo(sb, args);
		return sb.append(')').toString();
	}

	/** insert to overlay translation */
	public static String sqlinsert(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("overlay(");
		if(args.size() != 4) {
			throw new SQLException(GT.tr("{0} function takes four and only four arguments.", "insert"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0)).append(" placing ").append(args.get(3));
		sb.append(" from ").append(args.get(1)).append(" for ").append(args.get(2));
		return sb.append(')').toString();
	}

	/** lcase to lower translation */
	public static String sqllcase(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("lower(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "lcase"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** left to substring translation */
	public static String sqlleft(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("substring(");
		if(args.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "left"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0)).append(" for ").append(args.get(1));
		return sb.append(')').toString();
	}

	/** length translation */
	public static String sqllength(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("length(trim(trailing from ");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "length"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append("))").toString();
	}

	/** locate translation */
	public static String sqllocate(String name, List<Node> args) throws SQLException {
		if(args.size() == 2) {
			return "position(" + args.get(0) + " in " + args.get(1) + ")";
		}
		else if(args.size() == 3) {
			String tmp = "position(" + args.get(0) + " in substring(" + args.get(1) + " from " + args.get(2) + "))";
			return "(" + args.get(2) + "*sign(" + tmp + ")+" + tmp + ")";
		}
		else {
			throw new SQLException(GT.tr("{0} function takes two or three arguments.", "locate"), PSQLState.SYNTAX_ERROR);
		}
	}

	/** ltrim translation */
	public static String sqlltrim(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("trim(leading from ");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ltrim"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** position translation */
	public static String sqlposition(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("position(");
		if(args.size() != 3 && args.size() != 4) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "length"), PSQLState.SYNTAX_ERROR);
		}
		Joiner.on(' ').appendTo(sb, args.subList(0, 3));
		return sb.append(")").toString();
	}

/** right to substring translation */
	public static String sqlright(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("substring(");
		if(args.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "right"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0)).append(" from (length(").append(args.get(0)).append(")+1-").append(args.get(1));
		return sb.append("))").toString();
	}

	/** rtrim translation */
	public static String sqlrtrim(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("trim(trailing from ");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "rtrim"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** space translation */
	public static String sqlspace(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("repeat(' ',");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "space"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** substring to substr translation */
	public static String sqlsubstring(String name, List<Node> args) throws SQLException {
		if(args.size() == 2) {
			return "substr(" + args.get(0) + "," + args.get(1) + ")";
		}
		else if(args.size() == 3) {
			return "substr(" + args.get(0) + "," + args.get(1) + "," + args.get(2) + ")";
		}
		else {
			throw new SQLException(GT.tr("{0} function takes two or three arguments.", "substring"), PSQLState.SYNTAX_ERROR);
		}
	}

	/** ucase to upper translation */
	public static String sqlucase(String name, List<Node> args) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("upper(");
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ucase"), PSQLState.SYNTAX_ERROR);
		}
		sb.append(args.get(0));
		return sb.append(')').toString();
	}

	/** curdate to current_date translation */
	public static String sqlcurdate(String name, List<Node> args) throws SQLException {
		if(args.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curdate"), PSQLState.SYNTAX_ERROR);
		}
		return "current_date";
	}

	/** curtime to current_time translation */
	public static String sqlcurtime(String name, List<Node> args) throws SQLException {
		if(args.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curtime"), PSQLState.SYNTAX_ERROR);
		}
		return "current_time";
	}

	/** dayname translation */
	public static String sqldayname(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayname"), PSQLState.SYNTAX_ERROR);
		}
		return "to_char(" + args.get(0) + ",'Day')";
	}

	/** dayofmonth translation */
	public static String sqldayofmonth(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofmonth"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(day from " + args.get(0) + ")";
	}

	/**
	 * dayofweek translation adding 1 to postgresql function since we expect
	 * values from 1 to 7
	 */
	public static String sqldayofweek(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofweek"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(dow from " + args.get(0) + ")+1";
	}

	/** dayofyear translation */
	public static String sqldayofyear(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofyear"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(doy from " + args.get(0) + ")";
	}

	/** hour translation */
	public static String sqlhour(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "hour"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(hour from " + args.get(0) + ")";
	}

	/** minute translation */
	public static String sqlminute(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "minute"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(minute from " + args.get(0) + ")";
	}

	/** month translation */
	public static String sqlmonth(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "month"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(month from " + args.get(0) + ")";
	}

	/** monthname translation */
	public static String sqlmonthname(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "monthname"), PSQLState.SYNTAX_ERROR);
		}
		return "to_char(" + args.get(0) + ",'Month')";
	}

	/** quarter translation */
	public static String sqlquarter(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "quarter"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(quarter from " + args.get(0) + ")";
	}

	/** second translation */
	public static String sqlsecond(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "second"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(second from " + args.get(0) + ")";
	}

	/** week translation */
	public static String sqlweek(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "week"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(week from " + args.get(0) + ")";
	}

	/** year translation */
	public static String sqlyear(String name, List<Node> args) throws SQLException {
		if(args.size() != 1) {
			throw new SQLException(GT.tr("{0} function takes one and only one argument.", "year"), PSQLState.SYNTAX_ERROR);
		}
		return "extract(year from " + args.get(0) + ")";
	}

	/** time stamp add */
	public static String sqltimestampadd(String name, List<Node> args) throws SQLException {
		if(args.size() != 3) {
			throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampadd"), PSQLState.SYNTAX_ERROR);
		}
		String interval = SQLTextEscapeFunctions.constantToInterval(args.get(0).toString(), args.get(1).toString());
		StringBuilder sb = new StringBuilder();
		sb.append("(").append(interval).append("+");
		sb.append(args.get(2)).append(")");
		return sb.toString();
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
	public static String sqltimestampdiff(String name, List<Node> args) throws SQLException {
		if(args.size() != 3) {
			throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampdiff"), PSQLState.SYNTAX_ERROR);
		}
		String datePart = SQLTextEscapeFunctions.constantToDatePart(args.get(0).toString());
		StringBuilder sb = new StringBuilder();
		sb.append("extract( ").append(datePart).append(" from (").append(args.get(2)).append("-").append(args.get(1)).append("))");
		return sb.toString();
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
	public static String sqldatabase(String name, List<Node> args) throws SQLException {
		if(args.size() != 0) {
			throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "database"), PSQLState.SYNTAX_ERROR);
		}
		return "current_database()";
	}

	/** ifnull translation */
	public static String sqlifnull(String name, List<Node> args) throws SQLException {
		if(args.size() != 2) {
			throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "ifnull"), PSQLState.SYNTAX_ERROR);
		}
		return "coalesce(" + args.get(0) + "," + args.get(1) + ")";
	}

	/** user translation */
	public static String sqluser(String name, List<Node> args) throws SQLException {
		if(args.size() != 0) {
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
