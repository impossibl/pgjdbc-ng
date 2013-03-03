package com.impossibl.postgres.system.tables;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.impossibl.postgres.system.Version;


public class PgType implements Table<PgType.Row> {
	
	public static class Row {
		
		public int oid;
		public String name;
		public short length;
		public byte discriminator;
		public byte category;
		public byte deliminator;
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
		public byte alignment;
		public int baseTypeId;
		public int modId;
		public int numberOfDimension;
		
		
		public Row(ResultSet rs) throws SQLException {
			oid = rs.getInt("oid");;
			name = rs.getString("name");
			length = rs.getShort("length");
			discriminator = rs.getBytes("discriminator")[0];
			category = rs.getBytes("category")[0];
			deliminator = rs.getBytes("deliminator")[0];
			relationId = rs.getInt("relationId");
			elementTypeId = rs.getInt("elementTypeId");
			arrayTypeId = rs.getInt("arrayTypeId");
			inputId = rs.getInt("inputId");
			outputId = rs.getInt("outputId");
			receiveId = rs.getInt("receiveId");
			sendId = rs.getInt("sendId");
			modInId = rs.getInt("modInId");
			modOutId = rs.getInt("modOutId");
			analyzeId = rs.getInt("analyzeId");
			alignment = rs.getBytes("alignment")[0];
			baseTypeId = rs.getInt("baseTypeId");
			modId = rs.getInt("modId");
			numberOfDimension = rs.getInt("numberOfDimension");
		}
		
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
	
	public Row createRow(ResultSet resultSet) throws SQLException {
		return new Row(resultSet);
	}

	public static Object[] SQL = {
		Version.get(9,0,0),
		" select" +
		"		oid, typname as name, typlen as length, typtype as discriminator, typcategory as category, typdelim as deliminator, typrelid as relationId," +
		"		typelem as elementTypeId, typarray as arrayTypeId, typinput::oid as inputId, typoutput::oid as outputId, typreceive::oid as receiveId, typsend::oid as sendId," +
		"		typalign as alignment, typbasetype as baseTypeId, typndims as numberOfDimension" +
		" from" +
		"		pg_catalog.pg_type"
	};

}
