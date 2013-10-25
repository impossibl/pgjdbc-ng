/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import static java.util.Arrays.asList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;

import com.impossibl.postgres.jdbc.SQLTextTree.CompositeNode;
import com.impossibl.postgres.jdbc.SQLTextTree.GrammarPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.IdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.Node;
import com.impossibl.postgres.jdbc.SQLTextTree.NumericLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.ParenGroupNode;
import com.impossibl.postgres.jdbc.SQLTextTree.StringLiteralPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.UnquotedIdentifierPiece;
import com.impossibl.postgres.jdbc.SQLTextTree.WhitespacePiece;



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
  public final static String LOCATE = "locate"; // the 3 args version duplicate args
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

  public static Node invokeEscape(Method method, String name, List<Node> args) throws SQLException {
    try {
      return (Node) method.invoke(null, name, args);
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

  public static Node defaultEscape(String name, List<Node> args) throws SQLException {

    return call(name, args);
  }

  // ** numeric functions translations **
  /** rand to random translation */
  public static Node sqlrand(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ceiling"), PSQLState.SYNTAX_ERROR);
    }
    return call("random", args);
  }

  /** ceiling to ceil translation */
  public static Node sqlceiling(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ceiling"), PSQLState.SYNTAX_ERROR);
    }
    return call("ceil", args);
  }

  /** log to ln translation */
  public static Node sqllog(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log"), PSQLState.SYNTAX_ERROR);
    }
    return call("ln", args);
  }

  /** log10 to log translation */
  public static Node sqllog10(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "log10"), PSQLState.SYNTAX_ERROR);
    }
    return call("log", args);
  }

  /** power to pow translation */
  public static Node sqlpower(String name, List<Node> args) throws SQLException {
    if(args.size() != 2) {
      throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "power"), PSQLState.SYNTAX_ERROR);
    }
    return call("pow", args);
  }

  /** truncate to trunc translation */
  public static Node sqltruncate(String name, List<Node> args) throws SQLException {
    if(args.size() != 2) {
      throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "truncate"), PSQLState.SYNTAX_ERROR);
    }
    return call("trunc", args);
  }

  // ** string functions translations **
  /** char to chr translation */
  public static Node sqlchar(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "char"), PSQLState.SYNTAX_ERROR);
    }
    return call("chr", args);
  }

  /** concat translation */
  public static Node sqlconcat(String name, List<Node> args) {

    return groupedBy(args, "||");
  }

  /** insert to overlay translation */
  public static Node sqlinsert(String name, List<Node> args) throws SQLException {
    if(args.size() != 4) {
      throw new SQLException(GT.tr("{0} function takes four and only four arguments.", "insert"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("overlay",
        groupedSequence(args.get(0), "placing", args.get(3), "from", args.get(1), "for", args.get(2)));
  }

  /** lcase to lower translation */
  public static Node sqllcase(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "lcase"), PSQLState.SYNTAX_ERROR);
    }
    return call("lower", args);
  }

  /** left to substring translation */
  public static Node sqlleft(String name, List<Node> args) throws SQLException {
    if(args.size() != 2) {
      throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "left"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("substring",
        groupedSequence(args.get(0), "for", args.get(1)));
  }

  /** length translation */
  public static Node sqllength(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "length"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("length",
        groupedSequence(
            sequence("trim",
                groupedSequence("trailing", "from", args.get(0)))));
  }

  /** locate translation */
  public static Node sqllocate(String name, List<Node> args) throws SQLException {
    if(args.size() == 2) {
      return sequence("position",
          groupedSequence(args.get(0), "in", args.get(1)));
    }
    else if(args.size() == 3) {
      Node tmp = sequence("position",
          groupedSequence(args.get(0), "in", "substring", groupedSequence(args.get(1), "from", args.get(2))));
      return groupedSequence(args.get(2), grammar("*"), ident("sign"), groupedSequence(tmp), grammar("+"), tmp);
    }
    else {
      throw new SQLException(GT.tr("{0} function takes two or three arguments.", "locate"), PSQLState.SYNTAX_ERROR);
    }
  }

  /** ltrim translation */
  public static Node sqlltrim(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ltrim"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("trim",
        groupedSequence("leading", "from", args.get(0)));
  }

  /** position translation */
  public static Node sqlposition(String name, List<Node> args) throws SQLException {
    if(args.size() != 3 && args.size() != 4) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "length"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("position",
        groupedBy(args.subList(0, 3), " "));
  }

/** right to substring translation */
  public static Node sqlright(String name, List<Node> args) throws SQLException {
    if(args.size() != 2) {
      throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "right"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("substring",
        groupedSequence(args.get(0), "from",
            groupedSequence("length", groupedSequence(args.get(0)), grammar("+"), literal(1), grammar("-"), args.get(1))));
  }

  /** rtrim translation */
  public static Node sqlrtrim(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "rtrim"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("trim",
        groupedSequence("trailing", "from", args.get(0)));
  }

  /** space translation */
  public static Node sqlspace(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "space"), PSQLState.SYNTAX_ERROR);
    }
    return call("repeat", asList(literal(" "), args.get(0)));
  }

  /** substring to substr translation */
  public static Node sqlsubstring(String name, List<Node> args) throws SQLException {
    if(args.size() == 2 || args.size() == 3) {
      return call("substr", args);
    }
    else {
      throw new SQLException(GT.tr("{0} function takes two or three arguments.", "substring"), PSQLState.SYNTAX_ERROR);
    }
  }

  /** ucase to upper translation */
  public static Node sqlucase(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "ucase"), PSQLState.SYNTAX_ERROR);
    }
    return call("upper", args);
  }

  /** curdate to current_date translation */
  public static Node sqlcurdate(String name, List<Node> args) throws SQLException {
    if(args.size() != 0) {
      throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curdate"), PSQLState.SYNTAX_ERROR);
    }
    return ident("current_date");
  }

  /** curtime to current_time translation */
  public static Node sqlcurtime(String name, List<Node> args) throws SQLException {
    if(args.size() != 0) {
      throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "curtime"), PSQLState.SYNTAX_ERROR);
    }
    return ident("current_time");
  }

  /** dayname translation */
  public static Node sqldayname(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayname"), PSQLState.SYNTAX_ERROR);
    }
    return call("to_char", asList(args.get(0), literal("Day")));
  }

  /** dayofmonth translation */
  public static Node sqldayofmonth(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofmonth"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("day", "from", args.get(0)));
  }

  /**
   * dayofweek translation adding 1 to postgresql function since we expect
   * values from 1 to 7
   */
  public static Node sqldayofweek(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofweek"), PSQLState.SYNTAX_ERROR);
    }
    return groupedSequence(sequence("extract", groupedSequence("dow", "from", args.get(0))), grammar("+"), literal(1));
  }

  /** dayofyear translation */
  public static Node sqldayofyear(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "dayofyear"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("doy", "from", args.get(0)));
  }

  /** hour translation */
  public static Node sqlhour(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "hour"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("hour", "from", args.get(0)));
  }

  /** minute translation */
  public static Node sqlminute(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "minute"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("minute", "from", args.get(0)));
  }

  /** month translation */
  public static Node sqlmonth(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "month"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("month", "from", args.get(0)));
  }

  /** monthname translation */
  public static Node sqlmonthname(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "monthname"), PSQLState.SYNTAX_ERROR);
    }
    return call("to_char", asList(args.get(0), literal("Month")));
  }

  /** quarter translation */
  public static Node sqlquarter(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "quarter"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("quarter", "from", args.get(0)));
  }

  /** second translation */
  public static Node sqlsecond(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "second"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("second", "from", args.get(0)));
  }

  /** week translation */
  public static Node sqlweek(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "week"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("week", "from", args.get(0)));
  }

  /** year translation */
  public static Node sqlyear(String name, List<Node> args) throws SQLException {
    if(args.size() != 1) {
      throw new SQLException(GT.tr("{0} function takes one and only one argument.", "year"), PSQLState.SYNTAX_ERROR);
    }
    return sequence("extract", groupedSequence("year", "from", args.get(0)));
  }

  /** time stamp add */
  public static Node sqltimestampadd(String name, List<Node> args) throws SQLException {
    if(args.size() != 3) {
      throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampadd"), PSQLState.SYNTAX_ERROR);
    }
    Node interval = constantToInterval(args.get(0).toString(), args.get(1));
    return groupedSequence(interval, grammar("+"), args.get(2));
  }

  private final static Node constantToInterval(String type, Node value) throws SQLException {
    if(!type.startsWith(SQL_TSI_ROOT))
      throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
    String shortType = type.substring(SQL_TSI_ROOT.length());
    if(SQL_TSI_DAY.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" day"), "as", "interval"));
    else if(SQL_TSI_SECOND.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" second"), "as", "interval"));
    else if(SQL_TSI_HOUR.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" hour"), "as", "interval"));
    else if(SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" minute"), "as", "interval"));
    else if(SQL_TSI_MONTH.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" month"), "as", "interval"));
    else if(SQL_TSI_QUARTER.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(groupedSequence(value, grammar("::int"), grammar("*"), literal(3)), grammar("||"), literal(" month"), "as", "interval"));
    else if(SQL_TSI_WEEK.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" week"), "as", "interval"));
    else if(SQL_TSI_YEAR.equalsIgnoreCase(shortType))
      return sequence("CAST", groupedSequence(value, grammar("||"), literal(" year"), "as", "interval"));
    else if(SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType))
      throw new SQLException(GT.tr("Interval {0} not yet implemented", "SQL_TSI_FRAC_SECOND"), PSQLState.SYNTAX_ERROR);
    else
      throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
  }

  /** time stamp diff */
  public static Node sqltimestampdiff(String name, List<Node> args) throws SQLException {
    if(args.size() != 3) {
      throw new SQLException(GT.tr("{0} function takes three and only three arguments.", "timestampdiff"), PSQLState.SYNTAX_ERROR);
    }
    Node datePart = constantToDatePart(args.get(0).toString());
    return sequence("extract", groupedSequence(datePart, "from", groupedSequence(args.get(2), grammar("-"), args.get(1))));
  }

  private final static Node constantToDatePart(String type) throws SQLException {
    if(!type.startsWith(SQL_TSI_ROOT))
      throw new SQLException(GT.tr("Interval {0} not yet implemented", type), PSQLState.SYNTAX_ERROR);
    String shortType = type.substring(SQL_TSI_ROOT.length());
    if(SQL_TSI_DAY.equalsIgnoreCase(shortType))
      return ident("day");
    else if(SQL_TSI_SECOND.equalsIgnoreCase(shortType))
      return ident("second");
    else if(SQL_TSI_HOUR.equalsIgnoreCase(shortType))
      return ident("hour");
    else if(SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
      return ident("minute");
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
  public static Node sqldatabase(String name, List<Node> args) throws SQLException {
    if(args.size() != 0) {
      throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "database"), PSQLState.SYNTAX_ERROR);
    }
    return call("current_database", args);
  }

  /** ifnull translation */
  public static Node sqlifnull(String name, List<Node> args) throws SQLException {
    if(args.size() != 2) {
      throw new SQLException(GT.tr("{0} function takes two and only two arguments.", "ifnull"), PSQLState.SYNTAX_ERROR);
    }
    return call("coalesce", args);
  }

  /** user translation */
  public static Node sqluser(String name, List<Node> args) throws SQLException {
    if(args.size() != 0) {
      throw new SQLException(GT.tr("{0} function doesn''t take any argument.", "user"), PSQLState.SYNTAX_ERROR);
    }
    return ident("user");
  }

  static Node space() {
    return new WhitespacePiece(" ", -1);
  }

  static Node grammar(String val) {
    return new GrammarPiece(val.toString(), -1);
  }

  static Node literal(Number val) {
    return new NumericLiteralPiece(val.toString(), -1);
  }

  static Node literal(String text) {
    return new StringLiteralPiece(text, -1);
  }

  private static Node ident(String name) {
    return new UnquotedIdentifierPiece(name, -1);
  }

  static Node call(String name, List<Node> args) {
    return sequence(name, groupedBy(args, ","));
  }

  static ParenGroupNode groupedBy(List<Node> args, String sep) {

    ParenGroupNode groupNode = new ParenGroupNode(-1);

    Iterator<Node> argsIter = args.iterator();
    while(argsIter.hasNext()) {
      groupNode.add(argsIter.next());
      if(argsIter.hasNext()) {
        groupNode.add(new GrammarPiece(sep, -1));
      }
    }

    return groupNode;
  }

  static ParenGroupNode groupedSequence(Object... args) {

    ParenGroupNode groupNode = new ParenGroupNode(-1);

    sequence(groupNode, asList(args));

    return groupNode;
  }

  static CompositeNode sequence(String identName, Object... args) {

    CompositeNode seqNode = new CompositeNode(-1);

    seqNode.add(ident(identName));
    sequence(seqNode, asList(args));

    return seqNode;
  }

  static CompositeNode sequence(Object... args) {

    CompositeNode seqNode = new CompositeNode(-1);

    sequence(seqNode, asList(args));

    return seqNode;
  }

  static Node concat(Node a, Node b) {

    if(a instanceof CompositeNode) {
      CompositeNode ac = (CompositeNode) a;
      if(b instanceof CompositeNode)
        ac.nodes.addAll(((CompositeNode) b).nodes);
      else
        ac.nodes.add(b);
      return a;
    }
    else if(b instanceof CompositeNode) {
      ((CompositeNode) b).nodes.add(0, a);
      return b;
    }
    else {
      CompositeNode c = new CompositeNode(-1);
      c.add(a);
      c.add(b);
      return c;
    }
  }

  static void sequence(CompositeNode seqNode, List<Object> args) {

    ListIterator<Object> argsIter = args.listIterator();
    while(argsIter.hasNext()) {

      Object obj = argsIter.next();

      Node lastNode = seqNode.getLastNode();
      if(lastNode instanceof WhitespacePiece == false && obj instanceof WhitespacePiece == false &&
          obj instanceof ParenGroupNode == false &&
          (lastNode instanceof IdentifierPiece || obj instanceof IdentifierPiece || obj instanceof String)) {
        seqNode.add(new WhitespacePiece(" ", -1));
      }

      if(obj instanceof Node) {
        seqNode.add((Node) obj);
      }
      else {

        seqNode.add(new UnquotedIdentifierPiece(obj.toString(), -1));
      }
    }

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
