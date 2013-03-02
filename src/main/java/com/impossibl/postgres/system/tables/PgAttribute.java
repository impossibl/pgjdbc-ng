package com.impossibl.postgres.system.tables;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.impossibl.postgres.system.Version;

public class PgAttribute implements Table<PgAttribute.Row> {
		
	public static class Row {

		public int relationId;
		public String name;
		public int typeId;
		public short length;
		public short number;
		public int numberOfDimensions;
		
		public Row(ResultSet rs) throws SQLException {
			relationId = rs.getInt("relationId");
			name = rs.getString("name");
			typeId = rs.getInt("typeId");
			length = rs.getShort("length");
			number = rs.getShort("number");
			numberOfDimensions = rs.getInt("numberOfDimensions");
		}
		
	}
	
	public static PgAttribute INSTANCE = new PgAttribute(); 
	
	private PgAttribute() {}
	
	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}


	public Row createRow(ResultSet rs) throws SQLException {
		return new Row(rs);
	}
	
	public static Object[] SQL = {
		Version.get(9, 0, 0),
		" select " +
		"		attrelid as relationId, attname as name, atttypid as typeId, attlen as length, attnum as number, attndims as numberOfDimensions" +
		" from" +
		"		pg_catalog.pg_attribute"
	};

	
}
