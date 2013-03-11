package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;


public class PgType implements Table<PgType.Row> {
	
	public static class Row {
		
		public int oid;
		public String name;
		public short length;
		public String discriminator;
		public String category;
		public String deliminator;
		public int relationId;
		public int elementTypeId;
		public int arrayTypeId;
		public int inputId;
		public int outputId;
		public int receiveId;
		public int sendId;
		public int modInId;
		public int modOutId;
		public int analyzeId;
		public String alignment;
		public int baseTypeId;
		public int modId;
		public int numberOfDimensions;
		
		@Override
		public boolean equals(Object val) {
			if(val == null)
				return false;
			if(val instanceof Integer)
				return oid == (Integer)val;
			if(val instanceof PgType)
				return oid == ((Row)val).oid;
			return false;
		}
		
		@Override
		public int hashCode() {
			return oid;
		}
		
	}
	
	public static PgType INSTANCE = new PgType(); 
	
	private PgType() {}
	
	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}
	
	public Row createRow() {
		return new Row();
	}

	public static Object[] SQL = {
		Version.get(9,0,0),
		" select" +
		"		oid, typname as \"name\", typlen as \"length\", typtype as \"discriminator\", typcategory as \"category\", typdelim as \"deliminator\", typrelid as \"relationId\"," +
		"		typelem as \"elementTypeId\", typarray as \"arrayTypeId\", typinput::oid as \"inputId\", typoutput::oid as \"outputId\", typreceive::oid as \"receiveId\", typsend::oid as \"sendId\"," +
		"		typalign as alignment, typbasetype as \"baseTypeId\", typndims as \"numberOfDimensions\"" +
		" from" +
		"		pg_catalog.pg_type"
	};

}
