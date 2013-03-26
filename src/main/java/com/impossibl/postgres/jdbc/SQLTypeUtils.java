package com.impossibl.postgres.jdbc;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;

class SQLTypeUtils {
	
	public static Class<?> mapType(Type sourceType, Map<String, Class<?>> typeMap) {
		
		Class<?> targetType;
		
		if(sourceType instanceof ArrayType) {
			
			Class<?> componentType = mapType(((ArrayType) sourceType).getElementType(), typeMap);
			
			targetType = Array.newInstance(componentType, 0).getClass();
		}
		else {
			
			targetType = sourceType.getJavaType();

			Class<?> mappedType = typeMap.get(sourceType.getName());
			if(mappedType != null) {
				targetType = mappedType;
			}
			
		}
		
		return targetType;
	}
	
	public static Object coerce(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		Class<?> targetType = mapType(sourceType, typeMap);
		
		return coerce(val, sourceType, targetType, typeMap, connection);
	}

	public static Object coerce(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
			
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
		else if(targetType == URL.class) {
			return coerceToURL(val);
		}
		else if(targetType == Blob.class) {
			return coerceToBlob(val, connection);
		}
		else if(targetType == byte[].class) {
			return coerceToBytes(val, sourceType, connection);
		}
		else if(targetType.isArray()) {
			return coerceToArray(val, sourceType, targetType, typeMap, connection);
		}
		else if(targetType == Struct.class) {
			return coerceToStruct(val, sourceType, typeMap, connection);
		}
		else if(targetType == Record.class) {
			return coerceToRecord(val, sourceType, typeMap, connection);
		}
		else if(SQLData.class.isAssignableFrom(targetType)) {
			return coerceToCustomType(val, sourceType, targetType, typeMap, connection);
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
		else if(val instanceof URL) {
			return (URL) val;
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
		else if(val instanceof Blob) {
			return (Blob) val;
		}
		
		throw createCoercionException(val.getClass(), Blob.class);
	}
	
	public static byte[] coerceToBytes(Object val, Type sourceType, Context context) throws SQLException {
		
		if(val == null) {
			return null;
		}
		else if(val instanceof byte[]) {
			return (byte[]) val;
		}
		else if(sourceType.getJavaType().isInstance(val)) {
			
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			
			try {
				sourceType.getBinaryCodec().encoder.encode(sourceType, buffer, val, context);
			}
			catch(IOException e) {
				throw createCoercionException(val.getClass(), byte[].class);
			}
			
			buffer.skipBytes(4);
			return buffer.readBytes(buffer.readableBytes()).array();
		}

		throw createCoercionException(val.getClass(), byte[].class);
	}

	public static Object coerceToArray(Object val, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		if(val == null) {
			return null;
		}
		
		return coerceToArray(val, 0, Array.getLength(val), type, targetType, typeMap, connection);
	}
	
	public static Object coerceToArray(Object val, int index, int count, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {	
		
		if(val == null) {
			return null;
		}
		else if(val instanceof PGArray) {
			coerceToArray(((PGArray) val).getValue(), type, targetType, typeMap, connection);
		}
		else if(val.getClass().isArray() && type instanceof ArrayType) {
			
			ArrayType arrayType = (ArrayType) type;
			Type elementType = arrayType.getElementType();
			Class<?> elementClass = targetType.getComponentType();
			
			Object dst;
			
			if(count == 0) {
				dst = Array.newInstance(targetType.getComponentType(), count);
			}
			else if(val.getClass().getComponentType() == targetType.getComponentType()) {
				
				dst = val;
			}
			else if(elementClass.isAssignableFrom(Array.get(val, 0).getClass())) {

				dst = Array.newInstance(targetType.getComponentType(), count);
				System.arraycopy(val, (int)index, dst, 0, count);
			}
			else {

				dst = Array.newInstance(targetType.getComponentType(), count);

				for(int c=index, end=index+count; c < end; ++c) {
					
					Array.set(dst, c, coerce(Array.get(val, c), elementType, elementClass, typeMap, connection));
					
				}
				
			}
			
			return dst;
		}
		
		throw createCoercionException(val.getClass(), targetType);
	}
	
	public static Struct coerceToStruct(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		if(val == null) {
			
			return null;
		}
		else if(val instanceof Struct) {
			
			return (Struct) val;
		}
		else if(val instanceof Record) {

			return new PGStruct(connection, ((Record) val).getType(), ((Record) val).getValues());
		}
		else if(SQLData.class.isInstance(val) && sourceType instanceof CompositeType) {
			
			CompositeType compType = (CompositeType) sourceType;
			
			PGSQLOutput out = new PGSQLOutput(connection, compType, typeMap);
			
			((SQLData)val).writeSQL(out);
			
			return new PGStruct(connection, compType, out.getAttributeValues());
		}
		
		throw createCoercionException(val.getClass(), Struct.class);
	}

	public static Record coerceToRecord(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		if(val == null) {
			
			return null;
		}
		else if(val instanceof Record) {
			
			return (Record) val;
		}
		else if(sourceType instanceof CompositeType) {
			
			CompositeType compType = (CompositeType) sourceType;
			
			Object[] attributeVals;
			
			if(val instanceof Struct) {
			
				Struct struct = (Struct) val;
				attributeVals = struct.getAttributes();
			}
			else if(SQLData.class.isInstance(val)) {
				
				PGSQLOutput out = new PGSQLOutput(connection, compType, typeMap);
				
				((SQLData)val).writeSQL(out);
				
				attributeVals = out.getAttributeValues();
			}
			else {
				
				throw createCoercionException(val.getClass(), Record.class);
			}
			
			return new Record(compType, attributeVals);
		}
		
		throw createCoercionException(val.getClass(), Struct.class);
	}

	public static Object coerceToCustomType(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {
		
		if(val == null) {
			
			return null;
		}
		else if(sourceType instanceof CompositeType) {
			
			CompositeType compType = (CompositeType) sourceType;
			
			Object[] attributeVals;
			
			if(val instanceof Struct) {
			
				Struct struct = (Struct) val;
				attributeVals = struct.getAttributes();
			}
			else if(val instanceof Record) {
				
				Record record = (Record) val;
				attributeVals = record.getValues();
			}
			else {
				
				throw createCoercionException(val.getClass(), targetType);
			}
			
			Object dst;
			try {
				dst = targetType.newInstance();
			}
			catch(InstantiationException | IllegalAccessException e) {
				throw createCoercionException(val.getClass(), targetType, e);
			}
			
			PGSQLInput in = new PGSQLInput(connection, compType, typeMap, attributeVals);
			
			((SQLData) dst).readSQL(in, compType.getName());
			
			return dst;
		}
		
		throw createCoercionException(val.getClass(), targetType);
	}
	
	public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType) {
		return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' is not supported");
	}
	
	public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType, Exception cause) {
		return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' failed", cause);
	}
	
}
