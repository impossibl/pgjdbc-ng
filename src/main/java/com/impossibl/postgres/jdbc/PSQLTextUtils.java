package com.impossibl.postgres.jdbc;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class PSQLTextUtils {

	public static boolean isTrue(String value) {
		return "on".equals(value);
	}

	public static boolean isFalse(String value) {
		return "off".equals(value);
	}

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

	public static String getGetSessionReadabilityText() {
		
		return "SHOW default_transaction_read_only";
	}

	public static String getSetSessionReadabilityText(boolean readOnly) {
		
		return "SET SESSION CHARACTERISTICS AS TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE");
	}

	public static String getGetSessionIsolationLevelText() {
		
		return "SHOW default_transaction_isolation";
	}

	public static String getSetSessionIsolationLevelText(int level) {

		return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLACTION LEVEL " + getIsolationLevelText(level);
	}

	public static String getBeginText() {
		return "BEGIN";
	}

	public static String getCommitText() {
		return "COMMIT";
	}

	public static String getRollbackText() {
		return "ROLLBACK";
	}

	public static String getRollbackToText(PSQLSavepoint savepoint) {
		return "ROLLBACK TO SAVEPOINT " + savepoint;
	}

	public static String getSetSavepointText(PSQLSavepoint savepoint) {
		return "SAVEPOINT " + savepoint;
	}

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

	/*
	 * Transforms JDBC SQL text into text suitable for use with PostgreSQL's
	 * native protocol.
	 * 
	 * Uses the PARAM_SEARCH_PATTERN to find, and ignore, string and comment
	 * sections and replaces ? placeholders with index based ones like $0,$1..$n
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

}
