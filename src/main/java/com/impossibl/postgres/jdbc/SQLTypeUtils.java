package com.impossibl.postgres.jdbc;

import static java.lang.Character.toUpperCase;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.Factory;

class SQLTypeUtils {
	
	public static Object coerce(Object val, Class<?> targetType, PGConnection connection) throws SQLException {
		
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
			return coerceToDate(val, connection);
		}
		else if(targetType == Time.class) {
			return coerceToTime(val, connection);
		}
		else if(targetType == Timestamp.class) {
			return coerceToTimestamp(val, connection);
		}
		else if(targetType.isArray()) {
			return coerceToArray(val, targetType, connection);
		}
		else if(URL.class == targetType) {
			return coerceToURL(val);
		}
		else if(Blob.class == targetType) {
			return coerceToBlob(val, connection);
		}
		
		throw createCoercionException(val.getClass(), targetType);
	}
	
	public static byte coerceToByte(Object val) throws SQLException {
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
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
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
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
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
			return ((Number)val).intValue();
		}
		else if(val instanceof String) {
			return Integer.parseInt((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1 : 0;
		}
		else if(val instanceof PGBlob) {
			return ((PGBlob)val).lo.oid;
		}
		
		throw createCoercionException(val.getClass(), int.class);
	}

	public static long coerceToLong(Object val) throws SQLException {
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
			return ((Number)val).longValue();
		}
		else if(val instanceof String) {
			return Long.parseLong((String) val);
		}
		else if(val instanceof Boolean) {
			return ((Boolean)val) ? 1 : 0;
		}
		else if(val instanceof PGBlob) {
			return ((PGBlob)val).lo.oid;
		}
		
		throw createCoercionException(val.getClass(), long.class);
	}

	public static float coerceToFloat(Object val) throws SQLException {
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
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
		
		if(val == null) {
			return 0;
		}
		else if(val instanceof Number) {
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
		
		if(val == null) {
			return null;
		}
		else if(val instanceof Number) {
			return new BigDecimal(val.toString());
		}
		else if(val instanceof String) {
			return new BigDecimal((String) val);
		}
		
		throw createCoercionException(val.getClass(), BigDecimal.class);
	}

	public static boolean coerceToBoolean(Object val) throws SQLException {
		
		if(val == null) {
			return false;
		}
		else if(val instanceof Number) {
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
		
		if(val == null) {
			return null;
		}
		else if(val instanceof  Number) {
			return ((Number)val).toString();
		}
		else if(val instanceof String) {
			return (String) val;
		}
		else if(val instanceof Boolean) {
			return val.toString();
		}
		else if(val instanceof URL) {
			return val.toString();
		}
		
		throw createCoercionException(val.getClass(), String.class);
	}
	
	public static Date coerceToDate(Object val, Context context) throws SQLException {
		
		if(val == null) {
			return null;
		}
		else if(val instanceof String) {
			try {
				return new Date(context.getDateFormatter().parseDateTime((String) val).toDate().getTime());
			}
			catch(IllegalArgumentException e) {
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
		
		if(val == null) {
			return null;
		}
		else if(val instanceof String) {
			try {
				return new Time(context.getTimeFormatter().parseLocalTime((String) val).getMillisOfDay());
			}
			catch(IllegalArgumentException e) {
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
		
		if(val == null) {
			return null;
		}
		else if(val instanceof String) {
			try {
				return new Timestamp(context.getTimestampFormatter().parseDateTime((String) val).toDate().getTime());
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
	
	public static Object coerceToArray(Object val, Class<?> arrayType, Context context) throws SQLException {
		
		if(val == null) {
			return null;
		}
		else if(val instanceof PGArray) {
			
			return ((PGArray) val).getValue();			
		}
		else if(val.getClass().isArray()) {
			
			if(val.getClass().getComponentType() == arrayType.getComponentType()) {
				return val;
			}
			else {
				//Copy to correct array type
				int arrayLength = Array.getLength(val);
				Object newArray = Array.newInstance(arrayType.getComponentType(), arrayLength);
				System.arraycopy(val, 0, newArray, 0, arrayLength);
				return newArray;
			}
		}
		else if(Map.class.isAssignableFrom(val.getClass())) {
						
			int arrayLength = Array.getLength(val);
			Object newArray = Array.newInstance(arrayType.getComponentType(), arrayLength);
			
			for(int c=0; c < arrayLength; ++c) {
				Array.set(newArray, c, ((Map<?,?>)val).get(c));
			}
			System.arraycopy(val, 0, newArray, 0, arrayLength);
			return newArray;
		}
		
		throw createCoercionException(val.getClass(), arrayType);
	}
	
	public static URL coerceToURL(Object val) throws SQLException {
		
		if(val == null) {
			return null;
		}
		else if(val instanceof String) {
			try {
				return new URL((String) val);
			}
			catch(MalformedURLException e) {
				throw createCoercionException(val.getClass(), URL.class, e);
			}
		}
		
		throw createCoercionException(val.getClass(), URL.class);
	}
	
	public static Blob coerceToBlob(Object val, PGConnection connection) throws SQLException {
		
		if(val == null) {
			return null;
		}
		else if(val instanceof Integer) {
			return new PGBlob(connection, (int)val);
		}
		else if(val instanceof Long) {
			return new PGBlob(connection, (int)(long)val);
		}
		
		throw createCoercionException(val.getClass(), Blob.class);
	}
	
	public static Object coerceToType(Object val, Type type, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
	
		if(type instanceof CompositeType) {
			return coerceToCustomType(val, (CompositeType)type, typeMap, connection);
		}
		else if(type instanceof ArrayType) {
			return coerceToArrayType(val, (ArrayType)type, typeMap, connection);
		}
		
		return coerce(val, type.getJavaType(), connection);
	}
	
	public static Object coerceToArrayType(Object val, ArrayType type, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		return coerceToArrayType(val, 0, Array.getLength(val), type, typeMap, connection);
	}
	
	public static Object coerceToArrayType(Object val, int index, int count, ArrayType type, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {	
		
		Class<?> elementType = typeMap.get(type.getElementType().getName());
		if(elementType == null) {
			elementType = type.getElementType().getJavaType();
		}
		
		Object dst = Array.newInstance(elementType, count);
		
		for(int c=index, end=index+count; c < end; ++c) {
			
			Array.set(dst, c, coerceToType(Array.get(val, c), type.getElementType(), typeMap, connection));
			
		}
		
		return dst;
	}
	
	public static Object coerceToCustomType(Object val, CompositeType type, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		Class<?> targetType = typeMap.get(type.getName());
		if(targetType == null)
			return val;
		
		@SuppressWarnings("unchecked")
		Map<String, Object> src = (Map<String, Object>) val;
		
		Object dst = Factory.createInstance(targetType, 0);
		
		for(CompositeType.Attribute attr : type.getAttributes()) {
			
			String name = attr.name;
			Object value = src.get(name);

			//Attempt to set via property method (setXXXX);
			try {
				Method method = targetType.getMethod("set" + toUpperCase(name.charAt(0)) + name.substring(1));
				if(method.getParameterTypes().length == 1) {
					try {
						method.invoke(dst, coerceToType(value, attr.type, typeMap, connection));
						continue;
					}
					catch(IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw createCoercionException(value.getClass(), method.getParameterTypes()[0], e);
					}
				}
			}
			catch(NoSuchMethodException e) {
			}
			
			//Attempt to set via field
			try {
				Field field = targetType.getField(name);
				try {
					field.set(dst, coerceToType(value, attr.type, typeMap, connection));
					continue;
				}
				catch(IllegalArgumentException | IllegalAccessException e) {
					throw createCoercionException(value.getClass(), field.getType(), e);
				}
			}
			catch(NoSuchFieldException e) {
			}

		}
		
		return dst;
	}
	
	public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType) {
		return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' is not supported");
	}
	
	public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType, Exception cause) {
		return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' failed", cause);
	}
	
}
