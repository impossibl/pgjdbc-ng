package com.impossibl.postgres.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;

import com.impossibl.postgres.system.Context;

public class PSQLTypeUtils {
	
	public static Object coerce(Object val, Class<?> targetType, Context context) throws SQLException {
		
		if(targetType.isInstance(val)) {
			return val;
		}
		else if(targetType == Byte.class || targetType == byte.class) {
			return coerceToByte(val);
		}
		else if(targetType == Short.class || targetType == short.class) {
			return coerceToShort(val);
		}
		else if(targetType == Integer.class || targetType == int.class) {
			return coerceToInt(val);
		}
		else if(targetType == Long.class || targetType == long.class) {
			return coerceToLong(val);
		}
		else if(targetType == Float.class || targetType == float.class) {
			return coerceToFloat(val);
		}
		else if(targetType == Double.class || targetType == double.class) {
			return coerceToDouble(val);
		}
		else if(targetType == BigDecimal.class) {
			return coerceToBigDecimal(val);
		}
		else if(targetType == Boolean.class || targetType == boolean.class) {
			return coerceToBoolean(val);
		}
		else if(targetType == String.class) {
			return coerceToString(val);
		}
		else if(targetType == Date.class) {
			return coerceToDate(val, context);
		}
		else if(targetType == Time.class) {
			return coerceToTime(val, context);
		}
		else if(targetType == Timestamp.class) {
			return coerceToTimestamp(val, context);
		}
		
		throw createCoercionException(val.getClass(), targetType);
	}
	
	public static byte coerceToByte(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).byteValue();
		}
		else if(val instanceof String) {
			return Byte.parseByte((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? (byte)1 : (byte)0;
		}
		
		throw createCoercionException(val.getClass(), byte.class);
	}

	public static short coerceToShort(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).shortValue();
		}
		else if(val instanceof String) {
			return Short.parseShort((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? (short)1 : (short)0;
		}
		
		throw createCoercionException(val.getClass(), short.class);
	}

	public static int coerceToInt(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).intValue();
		}
		else if(val instanceof String) {
			return Integer.parseInt((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1 : 0;
		}
		
		throw createCoercionException(val.getClass(), int.class);
	}

	public static long coerceToLong(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).longValue();
		}
		else if(val instanceof String) {
			return Long.parseLong((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1 : 0;
		}
		
		throw createCoercionException(val.getClass(), long.class);
	}

	public static float coerceToFloat(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).floatValue();
		}
		else if(val instanceof String) {
			return Float.parseFloat((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1.0f : 0.0f;
		}
		
		throw createCoercionException(val.getClass(), float.class);
	}

	public static double coerceToDouble(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).doubleValue();
		}
		else if(val instanceof String) {
			return Double.parseDouble((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1.0 : 0.0;
		}
		
		throw createCoercionException(val.getClass(), double.class);
	}

	public static BigDecimal coerceToBigDecimal(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return new BigDecimal(val.toString());
		}
		else if(val instanceof String) {
			return new BigDecimal((String) val);
		}
		
		throw createCoercionException(val.getClass(), BigDecimal.class);
	}

	public static boolean coerceToBoolean(Object val) throws SQLException {
		
		if(val instanceof Number) {
			return ((Number)val).byteValue() != 0;
		}
		else if(val instanceof String) {
			String str = ((String) val).toLowerCase();
			switch(str) {
			case "on":
			case "true":
			case "1":
				return true;
			default:
				return false;
			}
		}
		else if(val instanceof Boolean) {
			return (boolean) val;
		}
		
		throw createCoercionException(val.getClass(), boolean.class);
	}
	
	public static String coerceToString(Object val) throws SQLException {
		
		if(val instanceof  Number) {
			return ((Number)val).toString();
		}
		else if(val instanceof String) {
			return (String) val;
		}
		else if(val instanceof Boolean) {
			return val.toString();
		}
		
		throw createCoercionException(val.getClass(), String.class);
	}
	
	public static Date coerceToDate(Object val, Context context) throws SQLException {
		
		if(val instanceof String) {
			try {
				return new Date(context.getDateFormat().parse((String) val).getTime());
			}
			catch(ParseException e) {
				throw new SQLException("Cannot parse date: " + val.toString());
			}
		}
		else if(val instanceof Date) {
			return (Date) val;
		}
		else if(val instanceof Timestamp) {
			return new Date(((Timestamp)val).getTime());
		}
		
		throw createCoercionException(val.getClass(), Date.class);
	}
	
	public static Time coerceToTime(Object val, Context context) throws SQLException {
		
		if(val instanceof String) {
			try {
				return new Time(context.getTimeFormat().parse((String) val).getTime());
			}
			catch(ParseException e) {
				throw new SQLException("Cannot parse date: " + val.toString());
			}
		}
		else if(val instanceof Time) {
			return (Time)val;
		}
		else if(val instanceof Timestamp) {
			return new Time(((Timestamp)val).getTime());
		}
		
		throw createCoercionException(val.getClass(), Time.class);
	}
	
	public static Timestamp coerceToTimestamp(Object val, Context context) throws SQLException {
		
		if(val instanceof String) {
			try {
				return Timestamp.valueOf((String) val);
			}
			catch(IllegalArgumentException e) {
				throw new SQLException("Cannot parse date: " + val.toString(), e);
			}
		}
		else if(val instanceof Time) {
			return new Timestamp(((Time)val).getTime());
		}
		else if(val instanceof Timestamp) {
			return (Timestamp) val;
		}
		
		throw createCoercionException(val.getClass(), Timestamp.class);
	}
	
	public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType) {
		return new SQLException("Coercion from '" + srcType.getClass().getName() + "' to '" + dstType.getClass().getName() + "' is not supported");
	}
	
}
