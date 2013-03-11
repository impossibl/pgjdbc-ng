package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;


/**
 * A system table object.
 * 
 * Facilitates easy exploitation of the auto-mapping feature of the driver.
 * Providing a POJO type for the row and some matching SQL and a list of
 * instances can be fetched from the database with no other work.  This is
 * used for system queries.
 * 
 * @author kdubb
 *
 * @param <R> The row type this table uses
 */
public interface Table<R> {

	/**
	 * Returns the SQL that is needed to load all rows of this table.
	 * 
	 * @param currentVersion Current version of the server that will execute it
	 * @return SQL text of the query
	 */
	public String getSQL(Version currentVersion);

	/**
	 * Creates and instance of the row type of this table.
	 * 
	 * @return An instance of the table's row type
	 */
	public R createRow();

}
