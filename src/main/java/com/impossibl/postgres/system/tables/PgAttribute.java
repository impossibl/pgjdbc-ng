package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;

public class PgAttribute implements Table<PgAttribute.Row> {
		
	public static class Row {

		public int relationId;
		public String name;
		public int typeId;
		public short length;
		public short number;
		public int numberOfDimensions;
		
		public int getRelationId() {
			return relationId;
		}
		
		public void setRelationId(int relationId) {
			this.relationId = relationId;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public int getTypeId() {
			return typeId;
		}
		
		public void setTypeId(int typeId) {
			this.typeId = typeId;
		}
		
		public short getLength() {
			return length;
		}
		
		public void setLength(short length) {
			this.length = length;
		}
		
		public short getNumber() {
			return number;
		}
		
		
		public void setNumber(short number) {
			this.number = number;
		}
		
		public int getNumberOfDimensions() {
			return numberOfDimensions;
		}
		
		public void setNumberOfDimensions(int numberOfDimensions) {
			this.numberOfDimensions = numberOfDimensions;
		}
		
	}
	
	public static PgAttribute INSTANCE = new PgAttribute(); 
	
	private PgAttribute() {}
	
	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}


	public Row createRow() {
		return new Row();
	}
	
	public static Object[] SQL = {
		Version.get(9, 0, 0),
		" select " +
		"		attrelid as \"relationId\", attname as \"name\", atttypid as \"typeId\", attlen as \"length\", attnum as \"number\", attndims as \"numberOfDimensions\"" +
		" from" +
		"		pg_catalog.pg_attribute"
	};

	
}
