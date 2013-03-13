package com.impossibl.postgres.jdbc;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Utility functions for creating and transforming SQL text into and out of
 * PostgreSQL's native dialect.
 * 
 * @author kdubb
 *
 */
public class PSQLTextUtils {

	/**
	 * Tests the given value for equality to "true"
	 * 
	 * @param value Value to test
	 * @return true if the value is true
	 */
	public static boolean isTrue(String value) {
		return "on".equals(value);
	}

	/**
	 * Test the given value for equality to "false"
	 * 
	 * @param value Value to test
	 * @return true if the value is false
	 */
	public static boolean isFalse(String value) {
		return "off".equals(value);
	}

	/**
	 * Translates a JDBC isolation code to text
	 * 
	 * @param level Isolation level code to translate
	 * @return Text version of the given level code
	 * @throws RuntimeException
	 * 					If the level code is not recognized
	 */
	public static String getIsolationLevelText(int level) {

		switch (level) {
		case TRANSACTION_READ_UNCOMMITTED:
			return "READ UNCOMMITTED";
		case TRANSACTION_READ_COMMITTED:
			return "READ COMMITTED";
		case TRANSACTION_REPEATABLE_READ:
			return "REPEATABLE READ";
		case TRANSACTION_SERIALIZABLE:
			return "SERIALIZABLE";
		}

		throw new RuntimeException("unknown isolation level");
	}

	/**
	 * Translates a text isolation level to a JDBC code
	 *  
	 * @param level Name of level to translate
	 * @return Isolation level code
	 * @throws RuntimeException
	 * 					If the level is not recognized
	 */
	public static int getIsolationLevel(String level) {

		switch (level.toUpperCase()) {
		case "READ UNCOMMITTED":
			return TRANSACTION_READ_UNCOMMITTED;
		case "READ COMMITTED":
			return TRANSACTION_READ_COMMITTED;
		case "REPEATABLE READ":
			return TRANSACTION_REPEATABLE_READ;
		case "SERIALIZABLE":
			return TRANSACTION_SERIALIZABLE;
		}

		throw new RuntimeException("unknown isolation level");
	}

	/**
	 * Retrieves an SQL query for getting the current session's readability state
	 * 
	 * @return SQL text
	 */
	public static String getGetSessionReadabilityText() {
		
		return "SHOW default_transaction_read_only";
	}

	/**
	 * Retrieves an SQL query for setting the current session's readability state
	 * 
	 * @param readOnly
	 * @return SQL text
	 */
	public static String getSetSessionReadabilityText(boolean readOnly) {
		
		return "SET SESSION CHARACTERISTICS AS TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE");
	}

	/**
	 * Retrieves an SQL query for getting the current session's isolation level
	 * 
	 * @return
	 */
	public static String getGetSessionIsolationLevelText() {
		
		return "SHOW default_transaction_isolation";
	}

	/**
	 * Retrieves an SQL query for setting the current session's isolation level
	 * 
	 * @param level
	 * @return SQL text
	 */
	public static String getSetSessionIsolationLevelText(int level) {

		return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLACTION LEVEL " + getIsolationLevelText(level);
	}

	/**
	 * Retrieves text for beginning a transaction
	 * 
	 * @return SQL text
	 */
	public static String getBeginText() {
		return "BEGIN";
	}

	/**
	 * Retrieves text for committing a transaction
	 * 
	 * @return SQL text
	 */
	public static String getCommitText() {
		return "COMMIT";
	}

	/**
	 * Retrieves text for rolling back a transaction
	 * 
	 * @return SQL text
	 */
	public static String getRollbackText() {
		return "ROLLBACK";
	}

	/**
	 * Retrieves text for rolling back a transaction to a specific savepoint
	 * 
	 * @param savepoint Name of savepoint to rollback to
	 * @return SQL text
	 */
	public static String getRollbackToText(PSQLSavepoint savepoint) {
		return "ROLLBACK TO SAVEPOINT " + savepoint;
	}

	/**
	 * Retrieves text for setting a specific savepoint
	 *	 
	 * @return SQL text
	 * @return
	 */
	public static String getSetSavepointText(PSQLSavepoint savepoint) {
		return "SAVEPOINT " + savepoint;
	}

	/**
	 * Retrieves text for releasing a specific savepoint
	 * 
	 * @param savepoint
	 * @return
	 */
	public static String getReleaseSavepointText(PSQLSavepoint savepoint) {
		return "RELEASE SAVEPOINT " + savepoint;
	}

	/*
	 * Pattern that finds these things:
	 * 	> Double quoted strings (ignoring escaped double quotes)
	 * 	> Single quoted strings (ignoring escaped single quotes)
	 * 	> SQL comments... from "--" to end of line
	 *  > C-Style comments (including nested sections)
	 *  > ? Parameter Placements 
	 */
	private static final Pattern PARAM_SEARCH_PATTERN = Pattern
			.compile("(?:\"(?:[^\"\\\\]|\\\\.)*\")|(?:'(?:[^\"\\\\]|\\\\.)*')|(?:\\-\\-.*$)|(?:/\\*.*\\*/)|\\?", Pattern.MULTILINE);

	/**
	 * Transforms JDBC SQL text into text suitable for use with PostgreSQL's
	 * native protocol.
	 * 
	 * Uses the PARAM_SEARCH_PATTERN to find, and ignore, string and comment
	 * sections and replaces ? placeholders with index based ones like $0,$1..$n
	 * 
	 * @param sql SQL text to transform
	 * @return PostgreSQL native SQL text
	 */
	public static String getProtocolSQLText(String sql) {

		Matcher matcher = PARAM_SEARCH_PATTERN.matcher(sql);

		StringBuffer newSql = new StringBuffer();

		int paramId = 1;

		while (matcher.find()) {
			if (matcher.group().equals("?")) {
				matcher.appendReplacement(newSql, "");
				newSql.append("$" + paramId++);
			}
			else {
				matcher.appendReplacement(newSql, "$0");
			}
		}

		matcher.appendTail(newSql);

		return newSql.toString();
	}

	/*
	 * Pattern that finds these things:
	 * 	> Double quoted strings (ignoring escaped double quotes)
	 * 	> Single quoted strings (ignoring escaped single quotes)
	 * 	> SQL comments... from "--" to end of line
	 *  > C-Style comments (including nested sections)
	 *  > ? Parameter Placements 
	 */
	private static final Pattern STMT_END_SEARCH_PATTERN = Pattern
			.compile("(?:\"(?:[^\"\\\\]|\\\\.)*\")|(?:'(?:[^\"\\\\]|\\\\.)*')|(?:\\-\\-.*$)|(?:/\\*.*\\*/)|;", Pattern.MULTILINE);

	/**
	 * Splits an SQL text block into multiple statements.
	 * 
	 * Uses the STMT_END_SEARCH_PATTERN to find, and ignore, string and comment
	 * sections and replaces ';' and the gathered text to the list of statements
	 * 
	 * @param sqlText SQL text block
	 * @return List of SQL statements in the text block
	 */
	public static List<String> splitStatements(String sqlText) {
		
		List<String> statements = new ArrayList<>();
		
		Matcher matcher = STMT_END_SEARCH_PATTERN.matcher(sqlText);

		StringBuffer current = new StringBuffer();
		
		while (matcher.find()) {
			
			if (matcher.group().equals(";")) {
				
				//Break it into a separate the statement
				
				matcher.appendReplacement(current, "");
				
				String statement = current.toString().trim();
				if(!statement.isEmpty())
					statements.add(statement);
				
				current = new StringBuffer();
			}
			else {
				
				matcher.appendReplacement(current, "$0");
			}
		}

		matcher.appendTail(current);

		String lastStatement = current.toString().trim();
		if(!lastStatement.isEmpty())
			statements.add(lastStatement);

		return statements;
	}
	
	/**
	 * Appends a clause, provided as text, to the given SQL text.
	 * 
	 * @param sqlText Input SQL text
	 * @return SQL text with appended clause or null if sqlText cannot be
	 * 					appended to
	 */
	public static String appendClause(String sqlText, String clause) {
		
		List<String> statements = splitStatements(sqlText);
		if(statements.size() > 1)
			return null;
		
		String statement = statements.get(0);

		return statement + " " + clause;
	}
	
	/**
	 * Appends a RETURNING clause, containing only the provided columns, to the
	 * given SQL text.
	 * 
	 * @param sqlText Input SQL text
	 * @return SQL text with appended clause or null if sqlText cannot be
	 * 					appended to
	 */
	public static String appendReturningClause(String sqlText, List<String> columns) {
		
		return appendClause(sqlText, " RETURNING " + joinColumns(columns, " , "));
	}
	
	/**
	 * Appends a RETURNING * clause to the given SQL text.
	 * 
	 * @param sqlText Input SQL text
	 * @return SQL text with appended clause or null if sqlText cannot be
	 * 					appended to
	 */
	public static String appendReturningClause(String sqlText) {
		
		return appendClause(sqlText, " RETURNING *");
	}
	
	/**
	 * Joins a list of columns into a string
	 * 
	 * @param columns List of columns to join
	 * @param separator String to separate columns with
	 * @return Joined representation of columns
	 */
	public static String joinColumns(List<String> columns, String separator) {
		
		StringBuilder sb = new StringBuilder();
		Iterator<String> columnIter = columns.iterator();
		
		while(columnIter.hasNext()) {
			
			sb.append(columnIter.next());
			
			if(columnIter.hasNext()) {
				sb.append(separator);
			}
			
		}
		
		return sb.toString();
	}

}
