package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.types.Modifiers.LENGTH;
import static com.impossibl.postgres.types.Modifiers.PRECISION;
import static com.impossibl.postgres.types.Modifiers.SCALE;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.sql.ResultSetMetaData.columnNullableUnknown;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;



/**
 * Utility functions for determine JDBC meta-data based
 * on varying amounts of information about PostgreSQL's types
 * 
 * @author kdubb
 *
 */
class SQLTypeMetaData {
	
	public static boolean requiresQuoting(Type type) {
		
		int sqlType = getSQLType(type);
		switch(sqlType) {
		    case Types.BIGINT:
		    case Types.DOUBLE:
		    case Types.FLOAT:
		    case Types.INTEGER:
		    case Types.REAL:
		    case Types.SMALLINT:
		    case Types.TINYINT:
		    case Types.NUMERIC:
		    case Types.DECIMAL:
		        return false;
		}
		
		return true;
	}
	
	public static boolean isCurrency(Type type) {

		if(type.unwrap().getPrimitiveType() == PrimitiveType.Money) {
			return true;
		}
		
		return false;
	}

	public static boolean isCaseSensitive(Type type) throws SQLException {
		
		switch(type.getCategory()) {
		case Enumeration:
		case String:
			return true;
			
		default:
			return false;
		}		
	}

	public static boolean isAutoIncrement(Type type, CompositeType relType, int relAttrNum) {
		
		if(relType != null && relAttrNum > 0) {
			
			CompositeType.Attribute attr = relType.getAttribute(relAttrNum);
			if(attr != null && attr.autoIncrement)
				return true;
		}
		else if(type instanceof DomainType) {
			
			return ((DomainType)type).getDefaultValue().startsWith("nextval(");
		}
		
		return false;
	}
	
	public static int isNullable(Type type, CompositeType relType, int relAttrNum) {
		
		int nullable = isNullable(type);
		
		//Check the relation attribute for nullability
		if(relType != null && relAttrNum != 0) {
			CompositeType.Attribute attr = relType.getAttribute(relAttrNum);
			if(attr != null) {

				if(attr.nullable && nullable == columnNullableUnknown) {
					nullable = columnNullable;
				}
				else if(!attr.nullable) {
					nullable = columnNoNulls;
				}
				
			}
		}
		
		return nullable;
	}
	
	public static int isNullable(Type type) {

		//Check domain types for nullability
		if(type instanceof DomainType) {
			return ((DomainType) type).isNullable() ? columnNullable : columnNoNulls;
		}
		
		//Everything else... we just don't know
		return columnNullableUnknown;		
	}
	
	public static boolean isSigned(Type type) {
		
		return type.unwrap().getCategory() == Type.Category.Numeric;
	}
	
	public static Type getType(int sqlType, Registry reg) {
	
		switch(sqlType) {
		case Types.BOOLEAN:
		case Types.BIT:
			return reg.loadType("bool");
		case Types.SMALLINT:
			return reg.loadType("int2");
		case Types.INTEGER:
			return reg.loadType("int4");
		case Types.BIGINT:
			return reg.loadType("int8");
		case Types.REAL:
			return reg.loadType("float4");
		case Types.FLOAT:
		case Types.DOUBLE:
			return reg.loadType("float8");
		case Types.NUMERIC:
		case Types.DECIMAL:
			return reg.loadType("numeric");
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return reg.loadType("text");
		case Types.TIME:
			return reg.loadType("time");
		case Types.DATE:
			return reg.loadType("date");
		case Types.TIMESTAMP:
			return reg.loadType("timestamp");
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return reg.loadType("bytea");
		case Types.ARRAY:
			return reg.loadType("anyarray");
		case Types.BLOB:
		case Types.CLOB:
			return reg.loadType("oid");
		case Types.DISTINCT:
			return null;
		case Types.STRUCT:
			return reg.loadType("oid");
		case Types.ROWID:
			return reg.loadType("oid");
		case Types.TINYINT:
		case Types.REF:
		case Types.OTHER:
		case Types.DATALINK:
		case Types.JAVA_OBJECT:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NCLOB:
		case Types.SQLXML:
		default:
			return null;
		}
		
	}
	
	public static int getSQLTypeIndex(int sqlType) {
		return (((sqlType % 255) + 255) % 255);
	}
	
	private static PrimitiveType[][] sqlToPrimitiveMatrix;
	private static int[] primitiveToSQLTypeMatrix;
	static {
		primitiveToSQLTypeMatrix = new int[255];
		primitiveToSQLTypeMatrix[PrimitiveType.Bool.ordinal()] = Types.BOOLEAN;
		primitiveToSQLTypeMatrix[PrimitiveType.Int2.ordinal()] = Types.SMALLINT;
		primitiveToSQLTypeMatrix[PrimitiveType.Int4.ordinal()] = Types.INTEGER;
		primitiveToSQLTypeMatrix[PrimitiveType.Int8.ordinal()] = Types.BIGINT;
		primitiveToSQLTypeMatrix[PrimitiveType.Float.ordinal()] = Types.REAL;
		primitiveToSQLTypeMatrix[PrimitiveType.Double.ordinal()] = Types.DOUBLE;
		primitiveToSQLTypeMatrix[PrimitiveType.Numeric.ordinal()] = Types.NUMERIC;
		primitiveToSQLTypeMatrix[PrimitiveType.Money.ordinal()] = Types.OTHER;
		primitiveToSQLTypeMatrix[PrimitiveType.String.ordinal()] = Types.VARCHAR;
		primitiveToSQLTypeMatrix[PrimitiveType.Date.ordinal()] = Types.DATE;
		primitiveToSQLTypeMatrix[PrimitiveType.Time.ordinal()] = Types.TIME;
		primitiveToSQLTypeMatrix[PrimitiveType.TimeTZ.ordinal()] = Types.TIME;
		primitiveToSQLTypeMatrix[PrimitiveType.Timestamp.ordinal()] = Types.TIMESTAMP;
		primitiveToSQLTypeMatrix[PrimitiveType.TimestampTZ.ordinal()] = Types.TIMESTAMP;
		primitiveToSQLTypeMatrix[PrimitiveType.Oid.ordinal()] = Types.ROWID;
		primitiveToSQLTypeMatrix[PrimitiveType.Array.ordinal()] = Types.ARRAY;
		primitiveToSQLTypeMatrix[PrimitiveType.Record.ordinal()] = Types.STRUCT;
		primitiveToSQLTypeMatrix[PrimitiveType.Domain.ordinal()] = Types.DISTINCT;
		primitiveToSQLTypeMatrix[PrimitiveType.Binary.ordinal()] = Types.BINARY;
		primitiveToSQLTypeMatrix[PrimitiveType.Bits.ordinal()] = Types.OTHER;
		primitiveToSQLTypeMatrix[PrimitiveType.Range.ordinal()] = Types.OTHER;
		primitiveToSQLTypeMatrix[PrimitiveType.UUID.ordinal()] = Types.OTHER;
		primitiveToSQLTypeMatrix[PrimitiveType.Interval.ordinal()] = Types.OTHER;
		primitiveToSQLTypeMatrix[PrimitiveType.Unknown.ordinal()] = Types.OTHER;
		
		sqlToPrimitiveMatrix = new PrimitiveType[255][];
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.NULL)] 					= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.BOOLEAN)]				= new PrimitiveType[] {PrimitiveType.Bool};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.BIT)] 						= new PrimitiveType[] {PrimitiveType.Bool};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.TINYINT)] 				= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.SMALLINT)] 			= new PrimitiveType[] {PrimitiveType.Int2};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.INTEGER)] 				= new PrimitiveType[] {PrimitiveType.Int4};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.BIGINT)] 				= new PrimitiveType[] {PrimitiveType.Int8};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.REAL)] 					= new PrimitiveType[] {PrimitiveType.Float};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.FLOAT)] 					= new PrimitiveType[] {PrimitiveType.Float};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.DOUBLE)] 				= new PrimitiveType[] {PrimitiveType.Double};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.NUMERIC)] 				= new PrimitiveType[] {PrimitiveType.Numeric};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.DECIMAL)] 				= new PrimitiveType[] {PrimitiveType.Numeric};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.CHAR)] 					= new PrimitiveType[] {PrimitiveType.String};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.VARCHAR)] 				= new PrimitiveType[] {PrimitiveType.String};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.LONGNVARCHAR)]		= new PrimitiveType[] {PrimitiveType.String};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.DATE)] 					= new PrimitiveType[] {PrimitiveType.Date};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.TIME)] 					= new PrimitiveType[] {PrimitiveType.Time, PrimitiveType.TimeTZ};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.TIMESTAMP)]			= new PrimitiveType[] {PrimitiveType.Timestamp, PrimitiveType.TimestampTZ};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.BINARY)] 				= new PrimitiveType[] {PrimitiveType.Binary};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.VARBINARY)] 			= new PrimitiveType[] {PrimitiveType.Binary};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.LONGVARBINARY)]	= new PrimitiveType[] {PrimitiveType.Binary};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.BLOB)]						= new PrimitiveType[] {PrimitiveType.Int4};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.CLOB)] 					= new PrimitiveType[] {PrimitiveType.Int4};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.SQLXML)] 				= new PrimitiveType[] {PrimitiveType.String};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.ARRAY)] 					= new PrimitiveType[] {PrimitiveType.Array};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.STRUCT)] 				= new PrimitiveType[] {PrimitiveType.Record};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.DISTINCT)] 			= new PrimitiveType[] {PrimitiveType.Domain};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.DATALINK)] 			= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.JAVA_OBJECT)] 		= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.NCHAR)] 					= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.NVARCHAR)] 			= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.LONGNVARCHAR)]		= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.REF)]						= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.ROWID)]					= new PrimitiveType[] {};
		sqlToPrimitiveMatrix[getSQLTypeIndex(Types.OTHER)]					= new PrimitiveType[] {};
	}
	
	public static int getSQLType(Type type) {
		
		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			return Types.OTHER;
		}

		return primitiveToSQLTypeMatrix[ptype.ordinal()];
	}
	
	public static int getSQLTypeAlias(int sqlType) {
		
		switch(sqlType) {
		case Types.BIT:
			return Types.BOOLEAN;
			
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return Types.VARCHAR;
			
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return Types.VARBINARY;
			
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return Types.NVARCHAR;
		}
		
		return sqlType;
	}
	
	public static String getTypeName(Type type, CompositeType relType, int relAttrNum) {
		
		//int4/int8 auto-increment fields -> serial/bigserial
		if(isAutoIncrement(type, relType, relAttrNum)) {
			
			switch(type.getPrimitiveType()) {
			case Int4:
				return "serial";
				
			case Int8:
				return "bigserial";
				
			default:
			}
			
		}
		
		return type.getName();
	}
	
	public static int getPrecisionRadix(Type type) {

		switch(type.unwrap().getCategory()) {
		case Numeric:
			return 10;
			
		case BitString:
			return 2;
			
		default:
			return 0;
		}
		
	}

	public static int getMinPrecision(Type type) {
		return 0;
	}
	
	public static int getMaxPrecision(Type type) {
		
		type = type.unwrap();

		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			return 0;
		}
		
		switch(ptype) {
		case Numeric:
			return 1000;
		case Time:
		case TimeTZ:
		case Timestamp:
		case TimestampTZ:
		case Interval:
			return 6;
		case String:
			return 10485760;
		case Bits:
			return 83886080;
		default:
			return 0;
		}
		
	}

	public static int getPrecision(Type type, int typeLength, int typeModifier) {

		type = type.unwrap();
		Map<String, Object> mods = type.getModifierParser().parse(typeModifier);

		//Lookup prec & length (if the mods have them)
		
		int precMod = -1;
		if(mods.containsKey(PRECISION)) {
			precMod = (int) mods.get(PRECISION);
		}
		
		int lenMod = -1;
		if(mods.containsKey(LENGTH)) {
			lenMod = (int) mods.get(LENGTH);
		}
		else if(typeLength != -1) {
			lenMod = typeLength;
		}
		//Calculate prec
		
		int prec = 0;
		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			prec = lenMod;
		}
		else {
			
			switch(ptype) {
			case Int2:
				prec = 5;
				break;
				
			case Int4:
			case Oid:
				prec = 10;
				break;
				
			case Int8:
			case Money:
				prec = 19;
				break;
				
			case Float:
				prec = 8;
				break;
				
			case Double:
				prec = 17;
				break;
				
			case Numeric:			
				if(precMod != 0) {
					prec = precMod;
				}
				else {
					prec = 131072;
				}
				break;
				
			case Date:
			case Time:
			case TimeTZ:
			case Timestamp:
			case TimestampTZ:
				prec = calculateDateTimeDisplaySize(type.getPrimitiveType(), precMod);
				break;
				
			case Interval:
				prec  = 49;
				break;
				
			case String:
			case Binary:
			case Bits:
				prec = lenMod;
				break;
				
			case Bool:
				prec = 1;
				break;
				
			case UUID:
				prec = 36;
				break;
				
			default:
				prec = lenMod;
			}
			
		}
		
		return prec;
	}

	public static int getMinScale(Type type) {
		
		type = type.unwrap();
		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			return 0;
		}
		
		switch(ptype) {
		case Money:
			return 2;
		default:
			return 0;
		}
		
	}
	
	public static int getMaxScale(Type type) {
		
		type = type.unwrap();
		
		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			return 0;
		}
		
		switch(ptype) {
		case Numeric:
			return 1000;
		default:
			return 0;
		}
		
	}

	public static int getScale(Type type, int typeLength, int typeModifier) {

		type = type.unwrap();
		
		Map<String, Object> mods = type.getModifierParser().parse(typeModifier);
		
		int scaleMod = -1;
		if(mods.get(SCALE) != null) {
			scaleMod = (int)mods.get(SCALE);
		}
		
		int scale = 0;
		
		switch(type.getPrimitiveType()) {
		case Float:
			scale = 8;
			break;

		case Double:
			scale = 17;
			break;

		case Numeric:
			if(scale == -1) {
				scale = 0;
			}
			else {
				scale = scaleMod;
			}
			break;

		case Time:
		case TimeTZ:
		case Timestamp:
		case TimestampTZ:
			int precMod = -1;
			if(mods.get(PRECISION) != null) {
				precMod = (int)mods.get(PRECISION);
			}
			
			if(precMod == -1) {
				scale = 6;
			}
			else {
				scale = precMod;
			}
			break;

		case Interval:
			if(scaleMod == -1) {
				scale = 6;
			}
			else {
				scale = scaleMod;
			}
			break;

		default:
			scale = 0;
		}
    
		return scale;
	}

	public static int getDisplaySize(Type type, int typeLength, int typeModifier) {

		type = type.unwrap();
		Map<String, Object> mods = type.getModifierParser().parse(typeModifier);
		
		int precMod = -1;
		if(mods.containsKey(PRECISION)) {
			precMod = (int) mods.get(PRECISION);
		}

		int lenMod = -1;
		if(mods.containsKey(LENGTH)) {
			lenMod = (int) mods.get(LENGTH);
		}
		else if(typeLength != -1) {
			lenMod = typeLength;
		}

		int size = 0;
		
		switch(type.getCategory()) {
		case Numeric:
			if(precMod == -1) {
				size = 131089;
			}
			else {
				int prec = getPrecision(type, typeLength, typeModifier);
				int scale = getScale(type, typeLength, typeModifier);
				size = prec + (scale != 0 ? 1 : 0) + 1;
			}
			break;
			
		case Boolean:
			size = 5; // true/false, yes/no, on/off, 1/0
			break;
			
		case String:
		case Enumeration:
		case BitString:
			if(lenMod == -1)
				size = Integer.MAX_VALUE;
			else
				size = lenMod;
			break;
			
		case DateTime:
			size = calculateDateTimeDisplaySize(type.getPrimitiveType(), precMod);
			break;
			
		case Timespan:
			size = 49;
			break;
			
		default:
			size = Integer.MAX_VALUE;
			break;
		}
		
		return size;
	}

	/**
	 * Calculates the display size for Dates, Times and Timestamps
	 * 
	 * NOTE: Values unceremoniously copied from previous JDBC driver
	 * 
	 * @param javaType Type to determine the display size of
	 * @param precision Precision modifier of type
	 * @return Suggested display size
	 */
	private static int calculateDateTimeDisplaySize(PrimitiveType primType, int precision) {

		if(primType == null)
			return 0;
		
		int size;
		
		switch(primType) {
		case Date:
			size = 13;
			break;
			
		case Time:
		case TimeTZ:
		case Timestamp:
		case TimestampTZ:
			
			int secondSize;
			switch(precision) {
			case -1:
				secondSize = 6+1;
				break;
			case 0:
				secondSize = 0;
				break;
			case 1:
				secondSize = 2+1;
				break;
			default:
				secondSize = precision+1;
				break;
			}
			
			switch(primType) {
			case Time:
				size = 8 + secondSize;
				break;
			case TimeTZ:
				size = 8 + secondSize + 6;
				break;
			case Timestamp:
				size = 13 + 1 + 8 + secondSize;
				break;
			case TimestampTZ:
				size = 13 + 1 + 8 + secondSize + 6;
				break;
			default:
				size = 0;
				//Can't happen...
			}
			break;
			
		default:
			size = 0;
		}
		
		return size;
	}

}
