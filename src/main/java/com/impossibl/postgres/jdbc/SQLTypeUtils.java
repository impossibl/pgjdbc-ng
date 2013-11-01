/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.Instants;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;
import static com.impossibl.postgres.jdbc.ArrayUtils.getDimensions;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import static java.math.RoundingMode.HALF_EVEN;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;


class SQLTypeUtils {

  public static Class<?> mapSetType(Type sourceType) {

    Class<?> targetType = sourceType.getJavaType(null);

    return targetType;
  }

  public static Class<?> mapGetType(Type sourceType, Map<String, Class<?>> typeMap, Context context) {

    Class<?> targetType = sourceType.getJavaType(typeMap);

    Class<?> mappedType = typeMap.get(sourceType.getName());
    if (mappedType != null) {
      targetType = mappedType;
    }
    else {

      switch(sourceType.getPrimitiveType()) {
        case Oid:
          if (sourceType.getName().equals(context.getSetting(BLOB_TYPE, BLOB_TYPE_DEFAULT))) {
            targetType = Blob.class;
          }
          break;

        case XML:
          targetType = SQLXML.class;
          break;

        case Time:
        case TimeTZ:
          targetType = Time.class;
          break;

        case Date:
          targetType = Date.class;
          break;

        case Timestamp:
        case TimestampTZ:
          targetType = Timestamp.class;
          break;

        default:
          break;
      }

    }

    return targetType;
  }

  public static Object coerce(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {

    return coerce(val, sourceType, targetType, typeMap, TimeZone.getDefault(), connection);
  }

  public static Object coerce(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, TimeZone zone, PGConnection connection) throws SQLException {

    if (val == null) {
      return null;
    }
    if (targetType.isInstance(val)) {
      return val;
    }
    else if (targetType == Byte.class || targetType == byte.class) {
      return coerceToByte(val);
    }
    else if (targetType == Short.class || targetType == short.class) {
      return coerceToShort(val);
    }
    else if (targetType == Integer.class || targetType == int.class) {
      return coerceToInt(val);
    }
    else if (targetType == Long.class || targetType == long.class) {
      return coerceToLong(val);
    }
    else if (targetType == Float.class || targetType == float.class) {
      return coerceToFloat(val);
    }
    else if (targetType == Double.class || targetType == double.class) {
      return coerceToDouble(val);
    }
    else if (targetType == BigDecimal.class) {
      return coerceToBigDecimal(val);
    }
    else if (targetType == Boolean.class || targetType == boolean.class) {
      return coerceToBoolean(val);
    }
    else if (targetType == String.class) {
      return coerceToString(val, connection);
    }
    else if (targetType == Date.class) {
      return coerceToDate(val, zone, connection);
    }
    else if (targetType == Time.class) {
      return coerceToTime(val, zone, connection);
    }
    else if (targetType == Timestamp.class) {
      return coerceToTimestamp(val, zone, connection);
    }
    else if (targetType == Instant.class) {
      return coerceToInstant(val, sourceType, zone, connection);
    }
    else if (targetType == URL.class) {
      return coerceToURL(val);
    }
    else if (targetType == Blob.class) {
      return coerceToBlob(val, connection);
    }
    else if (targetType == byte[].class) {
      return coerceToBytes(val, sourceType, connection);
    }
    else if (targetType.isArray()) {
      return coerceToArray(val, sourceType, targetType, typeMap, connection);
    }
    else if (targetType == Struct.class) {
      return coerceToStruct(val, sourceType, typeMap, connection);
    }
    else if (targetType == Record.class) {
      return coerceToRecord(val, sourceType, typeMap, connection);
    }
    else if (targetType == UUID.class) {
      return coerceToUUID(val, connection);
    }
    else if (SQLXML.class.isAssignableFrom(targetType)) {
      return coerceToXML(val, connection);
    }
    else if (SQLData.class.isAssignableFrom(targetType)) {
      return coerceToCustomType(val, sourceType, targetType, typeMap, connection);
    }

    throw createCoercionException(val.getClass(), targetType);
  }

  public static byte coerceToByte(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Byte) {
      return (byte) val;
    }
    else if (val instanceof Number) {

      try {
        return new BigDecimal(val.toString()).setScale(0, HALF_EVEN).byteValueExact();
      }
      catch (ArithmeticException e) {
        throw new SQLException("Coercion error", e);
      }
    }
    else if (val instanceof String) {
      return Byte.parseByte((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? (byte) 1 : (byte) 0;
    }

    throw createCoercionException(val.getClass(), byte.class);
  }

  public static short coerceToShort(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Short) {
      return (short) val;
    }
    else if (val instanceof Byte) {
      return (byte) val;
    }
    else if (val instanceof Number) {

      try {
        return new BigDecimal(val.toString()).setScale(0, HALF_EVEN).shortValueExact();
      }
      catch (ArithmeticException e) {
        throw new SQLException("Coercion error", e);
      }
    }
    else if (val instanceof String) {
      return Short.parseShort((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? (short) 1 : (short) 0;
    }

    throw createCoercionException(val.getClass(), short.class);
  }

  public static int coerceToInt(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Integer) {
      return (int) val;
    }
    else if (val instanceof Short) {
      return (short) val;
    }
    else if (val instanceof Byte) {
      return (byte) val;
    }
    else if (val instanceof Number) {

      try {
        return new BigDecimal(val.toString()).setScale(0, HALF_EVEN).intValueExact();
      }
      catch (ArithmeticException e) {
        throw new SQLException("Coercion error", e);
      }
    }
    else if (val instanceof String) {
      return Integer.parseInt((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    }
    else if (val instanceof PGBlob) {
      return ((PGBlob) val).lo.oid;
    }

    throw createCoercionException(val.getClass(), int.class);
  }

  public static long coerceToLong(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Long) {
      return (long) val;
    }
    else if (val instanceof Integer) {
      return (int) val;
    }
    else if (val instanceof Short) {
      return (short) val;
    }
    else if (val instanceof Byte) {
      return (byte) val;
    }
    else if (val instanceof Number) {

      try {
        return new BigDecimal(val.toString()).setScale(0, HALF_EVEN).longValueExact();
      }
      catch (ArithmeticException e) {
        throw new SQLException("Coercion error", e);
      }
    }
    else if (val instanceof String) {
      return Long.parseLong((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    }
    else if (val instanceof PGBlob) {
      return ((PGBlob) val).lo.oid;
    }

    throw createCoercionException(val.getClass(), long.class);
  }

  public static float coerceToFloat(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Float) {
      return (float) val;
    }
    else if (val instanceof Number) {
      return ((Number) val).floatValue();
    }
    else if (val instanceof String) {
      return Float.parseFloat((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1.0f : 0.0f;
    }

    throw createCoercionException(val.getClass(), float.class);
  }

  public static double coerceToDouble(Object val) throws SQLException {

    if (val == null) {
      return 0;
    }
    else if (val instanceof Double) {
      return (double) val;
    }
    else if (val instanceof Number) {
      return ((Number) val).doubleValue();
    }
    else if (val instanceof String) {
      return Double.parseDouble((String) val);
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1.0 : 0.0;
    }

    throw createCoercionException(val.getClass(), double.class);
  }

  public static BigDecimal coerceToBigDecimal(Object val) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    else if (val instanceof Number) {
      return new BigDecimal(val.toString());
    }
    else if (val instanceof Boolean) {
      return new BigDecimal((boolean) val ? "1.0" : "0.0");
    }
    else if (val instanceof String) {
      return new BigDecimal((String) val);
    }

    throw createCoercionException(val.getClass(), BigDecimal.class);
  }

  public static boolean coerceToBoolean(Object val) throws SQLException {

    if (val == null) {
      return false;
    }
    else if (val instanceof Boolean) {
      return (boolean) val;
    }
    else if (val instanceof Number) {
      return ((Number) val).byteValue() != 0;
    }
    else if (val instanceof String) {

      String str = ((String) val).toLowerCase();

      try {
        return Long.parseLong(str) != 0;
      }
      catch (Exception e) {
        // Ignore
      }

      try {
        return Double.parseDouble(str) != 0;
      }
      catch (Exception e) {
        // Ignore
      }

      switch(str) {
        case "on":
        case "true":
        case "t":
          return true;

        default:
          return false;
      }

    }

    throw createCoercionException(val.getClass(), boolean.class);
  }

  public static String coerceToString(Object val, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof String) {
      return (String) val;
    }
    else if (val instanceof Number) {
      return ((Number) val).toString();
    }
    else if (val instanceof Character) {
      return new String(new char[] {(Character) val });
    }
    else if (val instanceof Boolean) {
      return val.toString();
    }
    else if (val instanceof URL) {
      return val.toString();
    }
    else if (val instanceof Time) {
      return val.toString();
    }
    else if (val instanceof Date) {
      return val.toString();
    }
    else if (val instanceof Timestamp) {
      return val.toString();
    }
    else if (val instanceof Instant) {
      return ((Instant) val).disambiguate(TimeZone.getDefault()).print(context);
    }
    else if (val instanceof byte[]) {
      return new String((byte[]) val, context.getCharset());
    }
    else {
      return val.toString();
    }

  }

  public static Date coerceToDate(Object val, TimeZone zone, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Date) {
      return (Date) val;
    }
    else if (val instanceof Instant) {

      Instant inst = (Instant) val;

      if (inst.getType() != Instant.Type.Time) {

        return inst.switchTo(zone).toDate();

      }
    }

    throw createCoercionException(val.getClass(), Date.class);
  }

  public static Time coerceToTime(Object val, TimeZone zone, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Time) {
      return (Time) val;
    }
    else if (val instanceof Instant) {

      Instant inst = (Instant) val;

      if (inst.getType() != Instant.Type.Date && inst.getType() != Instant.Type.Infinity) {

        return ((Instant) val).switchTo(zone).toTime();

      }

    }

    throw createCoercionException(val.getClass(), Time.class);
  }

  public static Timestamp coerceToTimestamp(Object val, TimeZone zone, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Timestamp) {
      return (Timestamp) val;
    }
    else if (val instanceof Instant) {
      return ((Instant) val).switchTo(zone).toTimestamp();
    }

    throw createCoercionException(val.getClass(), Timestamp.class);
  }

  public static Instant coerceToInstant(Object val, Type sourceType, TimeZone zone, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Instant) {
      return ((Instant) val).disambiguate(zone);
    }
    else if (val instanceof Date) {
      return Instants.fromDate((Date) val, zone);
    }
    else if (val instanceof Time) {
      return Instants.fromTime((Time) val, zone);
    }
    else if (val instanceof Timestamp) {
      return Instants.fromTimestamp((Timestamp) val, zone);
    }
    else if (val instanceof String) {

      String str = (String) val;

      switch(sourceType.getPrimitiveType()) {

        case Date: {
          Map<String, Object> pieces = new HashMap<>();
          int offset = context.getDateFormatter().getParser().parse(str, 0, pieces);
          if (offset < 0) {
            throw createCoercionParseException(str, ~offset, Date.class);
          }
          return Instants.dateFromPieces(pieces, zone);
        }

        case Time:
        case TimeTZ: {
          Map<String, Object> pieces = new HashMap<>();
          int offset = context.getTimeFormatter().getParser().parse(val.toString(), 0, pieces);
          if (offset < 0) {
            throw createCoercionParseException(str, ~offset, Time.class);
          }
          return Instants.timeFromPieces(pieces, zone);
        }

        case Timestamp:
        case TimestampTZ: {
          Map<String, Object> pieces = new HashMap<>();
          int offset = context.getTimestampFormatter().getParser().parse(val.toString(), 0, pieces);
          if (offset < 0) {
            throw createCoercionParseException(str, ~offset, Timestamp.class);
          }
          return Instants.timestampFromPieces(pieces, zone);
        }

        default:
      }

    }

    throw createCoercionException(val.getClass(), Instant.class);
  }

  public static URL coerceToURL(Object val) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof URL) {
      return (URL) val;
    }
    else if (val instanceof String) {
      try {
        return new URL((String) val);
      }
      catch (MalformedURLException e) {
        throw createCoercionException(val.getClass(), URL.class, e);
      }
    }

    throw createCoercionException(val.getClass(), URL.class);
  }

  public static Blob coerceToBlob(Object val, PGConnection connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Blob) {
      return (Blob) val;
    }
    else if (val instanceof Integer) {
      return new PGBlob(connection, (int) val);
    }
    else if (val instanceof Long) {
      return new PGBlob(connection, (int) (long) val);
    }

    throw createCoercionException(val.getClass(), Blob.class);
  }

  public static byte[] coerceToBytes(Object val, Type sourceType, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof byte[]) {
      return (byte[]) val;
    }
    else if (val instanceof String) {
      return ((String)val).getBytes(context.getCharset());
    }
    else if (val instanceof PGSQLXML) {
      return ((PGSQLXML) val).getData();
    }
    else if (sourceType.getJavaType(Collections.<String, Class<?>> emptyMap()).isInstance(val)) {

      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

      try {
        sourceType.getBinaryCodec().encoder.encode(sourceType, buffer, val, context);
      }
      catch (IOException e) {
        throw createCoercionException(val.getClass(), byte[].class);
      }

      buffer.skipBytes(4);
      return buffer.readBytes(buffer.readableBytes()).array();
    }

    throw createCoercionException(val.getClass(), byte[].class);
  }

  public static Object coerceToArray(Object val, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof PGArray) {
      return coerceToArray(((PGArray) val).getValue(), type, targetType, typeMap, connection);
    }
    else if (val.getClass().isArray()) {
      return coerceToArray(val, 0, Array.getLength(val), type, targetType, typeMap, connection);
    }

    throw createCoercionException(val.getClass(), targetType);
  }

  public static Object coerceToArray(Object val, int index, int count, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof PGArray) {
      return coerceToArray(((PGArray) val).getValue(), index, count, type, targetType, typeMap, connection);
    }
    else if (val.getClass().isArray() && targetType.isArray()) {

      int targetDims = getDimensions(targetType);
      if (targetDims == 1) {

        targetDims = getDimensions(val);

        // Ensure targetType has correct # of dimensions
        targetType = Array.newInstance(targetType.getComponentType(), new int[targetDims]).getClass();
      }

      if (type instanceof ArrayType) {
        type = ((ArrayType) type).getElementType();
      }

      Class<?> elementClass = targetType.getComponentType();

      Object dst;

      if (count == 0) {
        dst = Array.newInstance(targetType.getComponentType(), count);
      }
      else if (val.getClass().getComponentType() == targetType.getComponentType()) {

        if (index == 0 && count == Array.getLength(val)) {
          dst = val;
        }
        else {
          dst = Arrays.copyOfRange((Object[]) val, index, index + count);
        }
      }
      else if (Array.get(val, 0) != null && elementClass.isAssignableFrom(Array.get(val, 0).getClass())) {

        dst = Array.newInstance(targetType.getComponentType(), count);

        for (int i = 0; i < count; ++i) {
          Array.set(dst, i, Array.get(val, i));
        }
      }
      else {

        dst = Array.newInstance(targetType.getComponentType(), count);

        for (int c = index, end = index + count; c < end; ++c) {

          Array.set(dst, c, coerce(Array.get(val, c), type, elementClass, typeMap, connection));

        }

      }

      return dst;
    }

    throw createCoercionException(val.getClass(), targetType);
  }

  public static Struct coerceToStruct(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {

    if (val == null) {

      return null;
    }
    else if (val instanceof Struct) {

      return (Struct) val;
    }
    else if (val instanceof Record) {

      return new PGStruct(connection, ((Record) val).getType(), ((Record) val).getValues());
    }
    else if (SQLData.class.isInstance(val) && sourceType instanceof CompositeType) {

      CompositeType compType = (CompositeType) sourceType;

      PGSQLOutput out = new PGSQLOutput(connection, compType);

      ((SQLData) val).writeSQL(out);

      return new PGStruct(connection, compType, out.getAttributeValues());
    }

    throw createCoercionException(val.getClass(), Struct.class);
  }

  public static Record coerceToRecord(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnection connection) throws SQLException {

    if (val == null) {

      return null;
    }
    else if (val instanceof Record) {

      return (Record) val;
    }
    else if (sourceType instanceof CompositeType) {

      CompositeType compType = (CompositeType) sourceType;

      Object[] attributeVals;

      if (val instanceof Struct) {

        Struct struct = (Struct) val;
        attributeVals = struct.getAttributes();
      }
      else if (SQLData.class.isInstance(val)) {

        PGSQLOutput out = new PGSQLOutput(connection, compType);

        ((SQLData) val).writeSQL(out);

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

    if (val == null) {

      return null;
    }
    else if (sourceType instanceof CompositeType) {

      CompositeType compType = (CompositeType) sourceType;

      Object[] attributeVals;

      if (val instanceof Struct) {

        Struct struct = (Struct) val;
        attributeVals = struct.getAttributes();
      }
      else if (val instanceof Record) {

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
      catch (InstantiationException | IllegalAccessException e) {
        throw createCoercionException(val.getClass(), targetType, e);
      }

      PGSQLInput in = new PGSQLInput(connection, compType, typeMap, attributeVals);

      ((SQLData) dst).readSQL(in, compType.getName());

      return dst;
    }

    throw createCoercionException(val.getClass(), targetType);
  }

  public static UUID coerceToUUID(Object val, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof UUID) {
      return (UUID) val;
    }
    else if (val instanceof String) {
      return UUID.fromString((String)val);
    }

    throw createCoercionException(val.getClass(), UUID.class);
  }

  public static SQLXML coerceToXML(Object val, PGConnection connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof SQLXML) {
      return (SQLXML) val;
    }
    if (val instanceof String) {

      return new PGSQLXML(connection, ((String)val).getBytes(connection.getCharset()));
    }
    else if (val instanceof byte[]) {

      return new PGSQLXML(connection, (byte[]) val);
    }

    throw createCoercionException(val.getClass(), SQLXML.class);
  }

  public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType) {
    return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' is not supported");
  }

  public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType, Exception cause) {
    return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' failed", cause);
  }

  public static SQLException createCoercionParseException(String val, int parseErrorPos, Class<?> dstType) {

    String errorText = "";
    int parseErrorEndPos = Math.min(parseErrorPos + 15, val.length());
    if (parseErrorEndPos < val.length()) {
      parseErrorEndPos -= 3;
      errorText = "...";
    }
    errorText = val.substring(parseErrorPos, parseErrorEndPos) + errorText;

    return new SQLException("Coercion from 'String' to '" + dstType.getName() + "' failed. Parser error near '" + errorText + "'");
  }

}
