package com.impossibl.postgres.system.tables;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.impossibl.postgres.system.Version;

public class PgProc implements Table<PgProc.Row> {
		
	public static class Row {

		public int oid;
		public String name;
		
		public Row(ResultSet rs) throws SQLException {
			oid = rs.getInt("oid");
			name = rs.getString("name");
		}
		
	}
	
	public static PgProc INSTANCE = new PgProc(); 
	
	private PgProc() {}
	
	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}


	public Row createRow(ResultSet rs) throws SQLException {
		return new Row(rs);
	}
	
	public static Object[] SQL = {
		Version.get(9, 0, 0),
		" select " +
		"		oid, proname as name" +
		" from" +
		"		pg_proc"
	};
	
}
