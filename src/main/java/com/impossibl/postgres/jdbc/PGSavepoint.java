package com.impossibl.postgres.jdbc;

import static com.google.common.base.Strings.nullToEmpty;

import java.sql.SQLException;
import java.sql.Savepoint;


/**
 * Reference to a savepoint
 * 
 * @author kdubb
 *
 */
class PGSavepoint implements Savepoint {

	private Integer id;
	private String name;
	private boolean released;

	PGSavepoint(int id) {
		this.id = id;
	}

	PGSavepoint(String name) {
		this.name = name;
	}

	/**
	 * Ensure the savepoint is valid
	 * 
	 * @throws SQLException
	 * 					If the connection is invalid
	 */
	void checkValid() throws SQLException {

		if(!isValid())
			throw new SQLException("Invalid savepoint");
	}

	@Override
	public int getSavepointId() throws SQLException {
		checkValid();
		
		if(id == null)
			throw new SQLException("named savepoints have no id");
		return id;
	}

	@Override
	public String getSavepointName() throws SQLException {
		checkValid();
		
		if(name == null)
			throw new SQLException("auto savepoints have no name");
		return name;
	}

	public String getId() {
		if(id != null)
			return "sp_" + id.toString();
		if(name != null)
			return Identifiers.escape(name);
		throw new IllegalStateException();
	}

	public boolean isValid() {
		return id != null || name != null;
	}

	public void invalidate() {
		id = null;
		name = null;
	}
	
	public boolean getReleased() {
		return released;
	}
	
	public void setReleased(boolean released) {
		this.released = released;
	}

	@Override
	public String toString() {
		return id != null ? id.toString() : nullToEmpty(name);
	}
	
}
