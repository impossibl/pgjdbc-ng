package com.impossibl.postgres.jdbc;

import java.sql.SQLException;

public class Exceptions {
	
	public static final SQLException NOT_SUPPORTED = new SQLException("Operation not supported"); 
	public static final SQLException NOT_IMPLEMENTED = new SQLException("Operation not implemented"); 
	public static final SQLException NOT_ALLOWED_ON_PREP_STMT = new SQLException("Operation not allowed on PreparedStatement");
	public static final SQLException INVALID_COMMAND_FOR_GENERATED_KEYS = new SQLException("SQL command does not support generated keys");
	public static final SQLException NO_RESULT_SET_AVAILABLE = new SQLException("No result set available");
	public static final SQLException NO_RESULT_COUNT_AVAILABLE = new SQLException("No result count available"); 
	public static final SQLException ILLEGAL_ARGUMENT = new SQLException("Illegal argument");
	public static final SQLException CLOSED_STATEMENT = new SQLException("Statement closed");
	public static final SQLException CLOSED_RESULT_SET = new SQLException("Result set closed");
	public static final SQLException CLOSED_CONNECTION = new SQLException("Connection closed");
	public static final SQLException INVALID_COLUMN_NAME = new SQLException("Invalid column name");
	public static final SQLException COLUMN_INDEX_OUT_OF_BOUNDS = new SQLException("Column index out of bounds");
	public static final SQLException PARAMETER_INDEX_OUT_OF_BOUNDS = new SQLException("Parameter index out of bounds");
	public static final SQLException SERVER_VERSION_NOT_SUPPORTED = new SQLException("Server version not supported");

}
