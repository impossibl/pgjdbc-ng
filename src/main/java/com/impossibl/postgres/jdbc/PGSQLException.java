package com.impossibl.postgres.jdbc;

import java.sql.SQLException;

/**
 * Postgres-specific SQL exception that carries and extended error information
 * reported by the server.
 * 
 * @author kdubb
 *
 */
public class PGSQLException extends SQLException {

	private static final long serialVersionUID = -176414268626933865L;
	
	private String schema;
	private String table;
	private String column;
	private String datatype;
	private String constraint;

	public PGSQLException() {
		super();
	}

	public PGSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

	public PGSQLException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public PGSQLException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public PGSQLException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public PGSQLException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public PGSQLException(String reason) {
		super(reason);
	}

	public PGSQLException(Throwable cause) {
		super(cause);
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public String getConstraint() {
		return constraint;
	}

	public void setConstraint(String constraint) {
		this.constraint = constraint;
	}

}
