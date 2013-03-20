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

	public static boolean isAutoIncrement(Type type) {
		
		if(type instanceof DomainType) {
			return ((DomainType)type).getDefaultValue().startsWith("nextval(");
		}
		
		return false;
	}
	
	public static int isNullable(Type type, CompositeType relType, int relAttrNum) {
		
		//Check the relation attribute for nullability
		if(relType != null && relAttrNum != 0) {
			CompositeType.Attribute attr = relType.getAttribute(relAttrNum);
			if(attr == null) {
				return columnNullableUnknown;
			}
			return attr.nullable ? columnNullable : columnNoNulls;
		}
		
		return isNullable(type);
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
	
	public static int getSQLType(Type type) {
		
		type = type.unwrap();
		
		PrimitiveType ptype = type.getPrimitiveType();
		if(ptype == null) {
			return Types.OTHER;
		}
		
		switch(ptype) {
		case Oid:
			return Types.INTEGER;
		case Bits:
			return Types.OTHER;
		case Bool:
			return Types.BOOLEAN;
		case Int2:
			return Types.SMALLINT;
		case Int4:
			return Types.INTEGER;
		case Int8:
			return Types.BIGINT;
		case Money:
			return Types.OTHER;
		case Float:
			return Types.FLOAT;
		case Double:
			return Types.DOUBLE;
		case Numeric:
			return Types.NUMERIC;
		case Date:
			return Types.DATE;
		case Time:
		case TimeTZ:
			return Types.TIME;
		case Timestamp:
		case TimestampTZ:
			return Types.TIMESTAMP;
		case Interval:
			return Types.OTHER;
		case String:
			return Types.VARCHAR;
		case Binary:
			return Types.BINARY;
		case UUID:
			return Types.OTHER;
		default:
			return Types.OTHER;
		}
			
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
		
		int precMod = 0;
		if(mods.containsKey(PRECISION)) {
			precMod = (int) mods.get(PRECISION);
		}
		
		int lenMod = 0;
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

		Map<String, Object> mods = type.unwrap().getModifierParser().parse(typeModifier);
		
		Object scale = mods.get(SCALE);
		if(scale == null)
			return 0;
		
		return (int) scale;
	}

	public static int getDisplaySize(Type type, int typeLength, int typeModifier) {

		type = type.unwrap();
		Map<String, Object> mods = type.getModifierParser().parse(typeModifier);
		
		int precMod = 0;
		if(mods.containsKey(PRECISION)) {
			precMod = (int) mods.get(PRECISION);
		}

		int lenMod = 0;
		if(mods.containsKey(LENGTH)) {
			lenMod = (int) mods.get(LENGTH);
		}
		else if(typeLength != -1) {
			lenMod = typeLength;
		}

		int size = 0;
		
		switch(type.getCategory()) {
		case Numeric:
			size = getPrecision(type, typeLength, typeModifier) + 1;
			break;
			
		case Boolean:
			size = 5; // true/false, yes/no, on/off, 1/0
			break;
			
		case String:
		case Enumeration:
		case BitString:
			if(lenMod == -1)
				size = 0;
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
			size = 0;
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
				size = 8 + precision + 6;
				break;
			case Timestamp:
				size = 13 + 1 + 8 + precision;
				break;
			case TimestampTZ:
				size = 13 + 1 + 8 + precision + 6;
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
