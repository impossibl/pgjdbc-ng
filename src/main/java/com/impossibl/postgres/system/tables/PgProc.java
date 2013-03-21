package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;


/**
 * Table for "pg_proc"
 * 
 * @author kdubb
 *
 */
public class PgProc implements Table<PgProc.Row> {

	public static class Row {

		public int oid;
		public String name;

	}

	public static final PgProc INSTANCE = new PgProc();

	private PgProc() {
	}

	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}

	public Row createRow() {
		return new Row();
	}

	public static final Object[] SQL = { Version.get(9, 0, 0), " select " + "		\"oid\", proname as \"name\"" + " from" + "		pg_proc" };

}
