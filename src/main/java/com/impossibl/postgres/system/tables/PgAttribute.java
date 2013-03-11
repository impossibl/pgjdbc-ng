package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;


/**
 * Table for "pg_attribute"
 * 
 * @author kdubb
 *
 */
public class PgAttribute implements Table<PgAttribute.Row> {

	public static class Row {

		public int relationId;
		public String name;
		public int typeId;
		public short length;
		public short number;
		public int numberOfDimensions;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + typeId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if(this == obj)
				return true;
			if(obj == null)
				return false;
			if(getClass() != obj.getClass())
				return false;
			Row other = (Row) obj;
			if(name == null) {
				if(other.name != null)
					return false;
			}
			else if(!name.equals(other.name))
				return false;
			if(typeId != other.typeId)
				return false;
			return true;
		}

	}

	public static PgAttribute INSTANCE = new PgAttribute();

	private PgAttribute() {
	}

	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}

	public Row createRow() {
		return new Row();
	}

	public static Object[] SQL = {
			Version.get(9, 0, 0),
			" select " + "		attrelid as \"relationId\", attname as \"name\", atttypid as \"typeId\", attlen as \"length\", attnum as \"number\", attndims as \"numberOfDimensions\""
					+ " from" + "		pg_catalog.pg_attribute" };

}
