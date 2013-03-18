package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.system.Version;


/**
 * Table for "pg_type"
 *  
 * @author kdubb
 *
 */
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
		public int domainBaseTypeId;
		public int domainTypeMod;
		public boolean domainNotNull;
		public int domainDimensions;
		public String namespace;
		public String domainDefault;

		@Override
		public boolean equals(Object val) {
			if(val == null)
				return false;
			if(val instanceof Integer)
				return oid == (Integer) val;
			if(val instanceof PgType)
				return oid == ((Row) val).oid;
			return false;
		}

		@Override
		public int hashCode() {
			return oid;
		}

	}

	public static PgType INSTANCE = new PgType();

	private PgType() {
	}

	public String getSQL(Version currentVersion) {
		return Tables.getSQL(SQL, currentVersion);
	}

	public Row createRow() {
		return new Row();
	}

	public static Object[] SQL = {
			Version.get(9, 0, 0),
			" select"	+
			"		t.oid, typname as \"name\", typlen as \"length\", typtype as \"discriminator\", typcategory as \"category\", typdelim as \"deliminator\", typrelid as \"relationId\"," +
			"		typelem as \"elementTypeId\", typarray as \"arrayTypeId\", typinput::oid as \"inputId\", typoutput::oid as \"outputId\", typreceive::oid as \"receiveId\", typsend::oid as \"sendId\","	+
			"		typmodin::oid as \"modInId\", typmodout::oid as \"modOutId\", typalign as alignment, n.nspname as \"namespace\", " +
			"		typbasetype as \"domainBaseTypeId\", typtypmod as \"domainTypeMod\", typnotnull as \"domainNotNull\", pg_catalog.pg_get_expr(typdefaultbin,0) as \"domainDefault\" " +	
			" from" +
			"		pg_catalog.pg_type t" +
			"	left join pg_catalog.pg_namespace n on (t.typnamespace = n.oid)"	
	};

}
