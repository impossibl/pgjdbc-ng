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

import static com.impossibl.postgres.jdbc.ArrayUtils.*;
import static com.impossibl.postgres.system.Settings.*;
import static java.math.RoundingMode.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Vector;

import javax.sql.rowset.serial.SQLInputImpl;
import javax.sql.rowset.serial.SQLOutputImpl;

import com.impossibl.postgres.api.data.Interval;
import com.impossibl.postgres.api.data.Path;
import com.impossibl.postgres.api.data.Record;
import com.impossibl.postgres.api.data.Tid;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.Instants;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.ByteStreams;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

class SQLTypeUtils {

  public static Class<?> mapSetType(Type sourceType) {
    return mapSetType(sourceType.getPreferredFormat(), sourceType);
  }

  public static Class<?> mapSetType(Format format, Type sourceType) {

    Class<?> targetType = sourceType.getJavaType(format, null);

    return targetType;
  }

  public static Class<?> mapGetType(Type sourceType, Map<String, Class<?>> typeMap, Context context) {
    return mapGetType(sourceType.getPreferredFormat(), sourceType, typeMap, context);
  }

  public static Class<?> mapGetType(Format format, Type sourceType, Map<String, Class<?>> typeMap, Context context) {

    Class<?> targetType = sourceType.getJavaType(format, typeMap);

    Class<?> mappedType = typeMap.get(sourceType.getName());
    if (mappedType != null) {
      targetType = mappedType;
    }
    else {

      switch (sourceType.getPrimitiveType()) {
      case Oid:
        if (sourceType.getName().equals(context.getSetting(BLOB_TYPE, BLOB_TYPE_DEFAULT))) {
          targetType = Blob.class;
        }
        if (sourceType.getName().equals(context.getSetting(CLOB_TYPE, CLOB_TYPE_DEFAULT))) {
          targetType = Clob.class;
        }
        break;

      case Tid:
        targetType = RowId.class;
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

      case Record:
        targetType = Struct.class;
        break;

      case Point:
      case Box:
      case Line:
      case LineSegment:
      case Circle:
        targetType = double[].class;
        break;
      case Path:
        targetType = Path.class;
        break;
      case Polygon:
        targetType = double[][].class;
        break;
      case Array:
        ArrayType arrayType = (ArrayType) sourceType;
        targetType = Array.newInstance(mapGetType(format, arrayType.getElementType(), typeMap, context), 0).getClass();
        break;
      default:
        break;
      }

    }

    return targetType;
  }

  public static Object coerce(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

    return coerce(sourceType.getPreferredFormat(), val, sourceType, targetType, typeMap, connection);
  }

  public static Object coerce(Format format, Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

    return coerce(format, val, sourceType, targetType, typeMap, TimeZone.getDefault(), connection);
  }

  public static Object coerce(Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, TimeZone zone, PGConnectionImpl connection) throws SQLException {

    return coerce(sourceType.getPreferredFormat(), val, sourceType, targetType, typeMap, zone, connection);
  }

  public static Object coerce(Format format, Object val, Type sourceType, Class<?> targetType, Map<String, Class<?>> typeMap, TimeZone zone, PGConnectionImpl connection) throws SQLException {

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
      return coerceToString(val, sourceType, connection);
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
    else if (targetType == OffsetDateTime.class) {
      return coerceToJavaTime(val, targetType, zone, connection);
    }
    else if (targetType == ZonedDateTime.class) {
      return coerceToJavaTime(val, targetType, zone, connection);
    }
    else if (targetType == java.time.Instant.class) {
      return coerceToJavaTime(val, targetType, zone, connection);
    }
    // TODO: Rest of java.time.*
    else if (targetType == Instant.class) {
      return coerceToInstant(val, sourceType, zone, connection);
    }
    else if (targetType == Interval.class) {
      return coerceToInterval(val);
    }
    else if (targetType == Class.class) {
      return coerceToClass(val);
    }
    else if (Enum.class.isAssignableFrom(targetType)) {
      return coerceToEnum(val, targetType);
    }
    else if (targetType == URL.class) {
      return coerceToURL(val);
    }
    else if (targetType == Blob.class) {
      return coerceToBlob(val, connection);
    }
    else if (targetType == Clob.class) {
      return coerceToClob(val, connection);
    }
    else if (targetType == RowId.class) {
      return coerceToRowId(val, sourceType);
    }
    else if (targetType == Tid.class) {
      return coerceToTid(val);
    }
    else if (InputStream.class.isAssignableFrom(targetType)) {
      return coerceToByteStream(format, val, sourceType, connection);
    }
    else if (targetType == byte[].class || targetType == Byte[].class) {
      return coerceToBytes(format, val, sourceType, connection);
    }
    else if (targetType.isArray()) {
      return coerceToArray(format, val, sourceType, targetType, typeMap, connection);
    }
    else if (targetType == Struct.class) {
      return coerceToStruct(val, sourceType, typeMap, connection);
    }
    else if (targetType == Record.class) {
      return coerceToRecord(format, val, sourceType, typeMap, zone, connection);
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
    else if (val instanceof String && sourceType.isParameterFormatSupported(Format.Text)) {
      try {
        return sourceType.getCodec(Format.Text).decoder.decode(sourceType, sourceType.getLength(), null, val, connection);
      }
      catch (IOException e) {
        // fall-thru
      }
    }

    throw createCoercionException(val.getClass(), targetType, val);
  }

  @SuppressWarnings("unchecked")
  private static Enum<?> coerceToEnum(Object val, Class<?> targetType) throws SQLException {
    if (val == null) {
      return null;
    }

    if (val instanceof String) {
      final String name = (String) val;
      try {
        // TODO: Could get expensive -- Cache these?
        return Enum.valueOf((Class<Enum>) targetType, name);
      } catch (final Exception e) {
        throw createCoercionException(val.getClass(), Class.class, val, e);
      }
    }

    throw createCoercionException(val.getClass(), Class.class, val);
  }

  private static Class<?> coerceToClass(final Object val) throws SQLException {
    if (val == null) {
      return null;
    }

    if (val instanceof String) {
      final String className = (String) val;
      try {
        return Class.forName(className);
      } catch (final Exception e) {
        throw createCoercionException(val.getClass(), Class.class, val, e);
      }
    }

    throw createCoercionException(val.getClass(), Class.class, val);

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
      return Byte.parseByte(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? (byte) 1 : (byte) 0;
    }

    throw createCoercionException(val.getClass(), byte.class, val);
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
      return Short.parseShort(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? (short) 1 : (short) 0;
    }

    throw createCoercionException(val.getClass(), short.class, val);
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
      return Integer.parseInt(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    }
    else if (val instanceof PGBlob) {
      return ((PGBlob) val).lo.oid;
    }
    else if (val instanceof PGClob) {
      return ((PGClob) val).lo.oid;
    }

    throw createCoercionException(val.getClass(), int.class, val);
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
      return Long.parseLong(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    }
    else if (val instanceof PGBlob) {
      return ((PGBlob) val).lo.oid;
    }
    else if (val instanceof Instant) {
      return ((Instant) val).toTimestamp().getTime();
    }

    throw createCoercionException(val.getClass(), long.class, val);
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
      return Float.parseFloat(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1.0f : 0.0f;
    }

    throw createCoercionException(val.getClass(), float.class, val);
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
      return Double.parseDouble(((String) val).trim());
    }
    else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1.0 : 0.0;
    }

    throw createCoercionException(val.getClass(), double.class, val);
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
      return new BigDecimal(((String) val).trim());
    }

    throw createCoercionException(val.getClass(), BigDecimal.class, val);
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

      String str = (((String) val).trim()).toLowerCase();

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

      switch (str) {
      case "on":
      case "true":
      case "t":
        return true;

      default:
        return false;
      }

    }

    throw createCoercionException(val.getClass(), boolean.class, val);
  }

  public static String coerceToString(Object val, Type type, Context context) throws SQLException {

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
    else if (val instanceof Interval) {
      return val.toString();
    }
    else if (val instanceof Instant) {
      return ((Instant) val).disambiguate(TimeZone.getDefault()).print(context);
    }
    else if (val instanceof PGSQLXML) {
      return ((PGSQLXML) val).getString();
    }
    else if (val instanceof Class) {
      return ((Class) val).getTypeName();
    }
    else if (val instanceof Enum) {
      return ((Enum) val).name();
    }
    else if (type.isResultFormatSupported(Format.Text)) {
      return coerceToStringFromType(val, type, context);
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

    throw createCoercionException(val.getClass(), Date.class, val);
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
    else if (val instanceof String) {
      return Time.valueOf(((String) val).trim());
    }

    throw createCoercionException(val.getClass(), Time.class, val);
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
    else if (val instanceof String) {
      return Timestamp.valueOf(((String) val).trim());
    }

    throw createCoercionException(val.getClass(), Timestamp.class, val);
  }

  public static Object coerceToJavaTime(Object val, final Class<?> targetType,
      final TimeZone zone, final Context context) throws SQLException {

    if (val == null) {
      return null;
    }

    if (val instanceof Instant) {
      final Instant i = (Instant) val;

      // just make it a timestamp and let the next block handle it
      final Timestamp ts = i.toTimestamp();
      val=ts;
    }

    if (val instanceof Timestamp) {
      final Timestamp ts = (Timestamp) val;

      if (targetType.isAssignableFrom(java.time.Instant.class)) {
        return ts.toInstant();
      }

      final LocalDateTime localDateTime = ts.toLocalDateTime();
      if (targetType.isAssignableFrom(LocalDateTime.class)) {
        return localDateTime;
      }

      if (targetType.isAssignableFrom(ZonedDateTime.class)) {
        final ZoneId zoneId = zone.toZoneId();
        return localDateTime.atZone(zoneId);
      }

      if (targetType.isAssignableFrom(OffsetDateTime.class)) {
        final int offsetMillis = zone.getRawOffset();
        final ZoneOffset offset = ZoneOffset.ofTotalSeconds(offsetMillis / 1000);
        return localDateTime.atOffset(offset);
      }
    }

    if (val instanceof String) {
      final String s = (String) val;
      final java.time.Instant instant = java.time.Instant.parse(s);

      if (targetType.isAssignableFrom(java.time.Instant.class)) {
        return instant;
      }

      if (targetType.isAssignableFrom(ZonedDateTime.class)) {
        return ZonedDateTime.parse(s);
      }

      if (targetType.isAssignableFrom(OffsetDateTime.class)) {
        return OffsetDateTime.parse(s);
      }
    }

    throw createCoercionException(val.getClass(), targetType, val);
  }

  public static Interval coerceToInterval(Object val) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Interval) {
      return (Interval) val;
    }
    else if (val instanceof String) {
      return new Interval((String) val);
    }

    throw createCoercionException(val.getClass(), Interval.class, val);
  }

  public static Instant coerceToInstant(final Object val, final Type sourceType, final TimeZone zone, final Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Instant) {
      return ((Instant) val).disambiguate(zone);
    }
    else if (val instanceof java.time.Instant) {
      final java.time.Instant jti = (java.time.Instant) val;
      return Instants.fromTimestamp(Timestamp.from(jti), zone);
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

      switch (sourceType.getPrimitiveType()) {

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

    throw createCoercionException(val.getClass(), Instant.class, val);
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
        throw createCoercionException(val.getClass(), URL.class, val, e);
      }
    }

    throw createCoercionException(val.getClass(), URL.class, val);
  }

  public static Blob coerceToBlob(Object val, PGConnectionImpl connection) throws SQLException {

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

    throw createCoercionException(val.getClass(), Blob.class, val);
  }

  public static RowId coerceToRowId(Object val, Type sourceType) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof RowId) {
      return (RowId) val;
    }
    else if (val instanceof Tid) {
      return new PGRowId((Tid) val);
    }

    throw createCoercionException(val.getClass(), RowId.class, val);
  }

  public static Tid coerceToTid(Object val) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Tid) {
      return (Tid) val;
    }
    else if (val instanceof PGRowId) {
      PGRowId rowId = (PGRowId) val;
      return rowId.tid;
    }

    throw createCoercionException(val.getClass(), Tid.class, val);
  }

  public static InputStream coerceToByteStream(Object val, Type sourceType, Context context) throws SQLException {

    return coerceToByteStream(sourceType.getPreferredFormat(), val, sourceType, context);
  }

  public static InputStream coerceToByteStream(Format format, Object val, Type sourceType, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof InputStream) {
      InputStream is = (InputStream) val;
      try {
        // Ensure stream is at beginning (in cases
        // where it is used multiple times)
        is.reset();
      }
      catch (IOException e) {
        throw new SQLException(e);
      }
      return is;
    }
    else if (val instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) val);
    }
    else if (val instanceof String) {
      return new ByteArrayInputStream(((String) val).getBytes(context.getCharset()));
    }
    else if (val instanceof PGSQLXML) {
      byte[] data = ((PGSQLXML) val).getData();
      return data != null ? new ByteArrayInputStream(data) : null;
    }
    else if (sourceType.getJavaType(format, Collections.<String, Class<?>> emptyMap()).isInstance(val)) {

      // Encode into byte array using type encoder

      final ByteBuf buffer = Unpooled.buffer();

      try {
        sourceType.getBinaryCodec().encoder.encode(sourceType, buffer, val, context);
      }
      catch (IOException e) {
        throw createCoercionException(val.getClass(), byte[].class, val);
      }

      // Skip written length
      buffer.skipBytes(4);

      return new ByteBufInputStream(buffer) {
        @Override
        public void close() throws IOException {
          super.close();
          buffer.release();
        }
      };
    }

    throw createCoercionException(val.getClass(), byte[].class, val);
  }

  public static byte[] coerceToBytes(Object val, Type sourceType, Context context) throws SQLException {

    return coerceToBytes(sourceType.getPreferredFormat(), val, sourceType, context);
  }

  public static byte[] coerceToBytes(Format format, Object val, Type sourceType, Context context) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof InputStream) {
      try {
        return ByteStreams.toByteArray((InputStream) val);
      }
      catch (IOException e) {
        throw new SQLException(e);
      }
    }
    else if (val instanceof byte[]) {
      return (byte[]) val;
    }
    else if (val instanceof String) {

      if (sourceType.isParameterFormatSupported(Format.Text) && sourceType.getTextCodec().decoder.getOutputType() == byte[].class) {
        try {
          return (byte[]) sourceType.getTextCodec().decoder.decode(sourceType, sourceType.getLength(), null, val, context);
        }
        catch (IOException e) {
          throw createCoercionException(val.getClass(), byte[].class, val);
        }
      }
      else {
        return ((String) val).getBytes(context.getCharset());
      }
    }
    else if (val instanceof PGSQLXML) {
      return ((PGSQLXML) val).getData();
    }
    else if (sourceType.getJavaType(format, Collections.<String, Class<?>> emptyMap()).isInstance(val)) {

      // Encode into byte array using type encoder

      ByteBuf buffer = Unpooled.buffer();

      try {
        sourceType.getBinaryCodec().encoder.encode(sourceType, buffer, val, context);
      }
      catch (IOException e) {
        throw createCoercionException(val.getClass(), byte[].class, val);
      }

      // Skip written length
      buffer.skipBytes(4);

      byte[] array = new byte[buffer.readableBytes()];
      buffer.readBytes(array);
      return array;
    }

    throw createCoercionException(val.getClass(), byte[].class, val);
  }

  public static Clob coerceToClob(Object val, PGConnectionImpl connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof Clob) {
      return (Clob) val;
    }
    else if (val instanceof Integer) {
      return new PGClob(connection, (int) val);
    }
    else if (val instanceof Long) {
      return new PGClob(connection, (int) (long) val);
    }

    throw createCoercionException(val.getClass(), Clob.class, val);
  }

  public static Object coerceToArray(Object val, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

    return coerceToArray(type.getPreferredFormat(), val, type, targetType, typeMap, connection);
  }

  public static Object coerceToArray(final Format format, final Object val, final Type type, final Class<?> targetType, final Map<String, Class<?>> typeMap, final PGConnectionImpl connection) throws SQLException {

    if (val == null) {
      return null;
    }

    if (val instanceof PGArray) {
      return coerceToArray(format, ((PGArray) val).getValue(), type, targetType, typeMap, connection);
    }

    if (val.getClass().isArray()) {
      return coerceToArray(format, val, 0, Array.getLength(val), type, targetType, typeMap, connection);
    }

    if (val instanceof Collection) {
      Object[] array = ((Collection) val).toArray();
      return coerceToArray(format, array, 0, array.length, type, targetType, typeMap, connection);
    }

    if (val instanceof SQLData) {
      final SQLData sqlData = (SQLData) val;
      return coerceFromSQLDataToArray(format, sqlData, type, targetType, typeMap, connection);
    }

    throw createCoercionException(val.getClass(), targetType, val);
  }

  private static Object coerceFromSQLDataToArray(final Format format, final SQLData sqlData, final Type type, final Class<?> targetType, final Map<String, Class<?>> typeMap, final PGConnectionImpl connection) throws SQLException {
    final Vector<?> attributes = new Vector<Object>();
    final SQLOutput stream = new SQLOutputImpl(attributes, typeMap);
    sqlData.writeSQL(stream);

    final Object[] val = attributes.toArray();
    return coerceToArray(format, val, type, targetType, typeMap, connection);
  }

  public static Object coerceToArray(Object val, int index, int count, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

    return coerceToArray(type.getPreferredFormat(), val, index, count, type, targetType, typeMap, connection);
  }

  public static Object coerceToArray(Format format, Object val, int index, int count, Type type, Class<?> targetType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

    if (val == null) {
      return null;
    }
    else if (val instanceof PGArray) {
      return coerceToArray(format, ((PGArray) val).getValue(), index, count, type, targetType, typeMap, connection);
    }
    else if (val.getClass() == type.getJavaType(format, typeMap) && targetType.isArray()) {

      Object array = Array.newInstance(targetType.getComponentType(), count);
      for (int c = index, end = index + count; c < end; ++c) {
        Array.set(array, c - index, coerce(format, Array.get(val, c), type, targetType.getComponentType(), typeMap, connection));
      }

      return array;
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

          Array.set(dst, c, coerce(format, Array.get(val, c), type, elementClass, typeMap, connection));

        }

      }

      return dst;
    }

    throw createCoercionException(val.getClass(), targetType, val);
  }

  public static Struct coerceToStruct(Object val, Type sourceType, Map<String, Class<?>> typeMap, PGConnectionImpl connection) throws SQLException {

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

      PGSQLOutputImpl out = new PGSQLOutputImpl(connection, compType);

      ((SQLData) val).writeSQL(out);

      return new PGStruct(connection, compType, out.getAttributeValues());
    }
    else if (val instanceof Object[] && sourceType instanceof CompositeType) {

      CompositeType compType = (CompositeType) sourceType;

      return new PGStruct(connection, compType, (Object[]) val);
    }

    throw createCoercionException(val.getClass(), Struct.class, val);
  }

  public static Record coerceToRecord(Format format, Object val, Type sourceType, Map<String, Class<?>> typeMap, TimeZone zone, PGConnectionImpl connection) throws SQLException {

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

        PGSQLOutputImpl out = new PGSQLOutputImpl(connection, compType);

        ((SQLData) val).writeSQL(out);

        attributeVals = out.getAttributeValues();
      }
      else {
        throw createCoercionException(val.getClass(), Record.class, val);
      }

      if (compType.getAttributes().size() != attributeVals.length) {
        throw createCoercionException(val.getClass(), Record.class, val);
      }

      for (int c = 0; c < attributeVals.length; ++c) {

        Type attrType = compType.getAttribute(c + 1).type;
        Class<?> attrTargetType = mapSetType(format, attrType);

        attributeVals[c] = coerce(format, attributeVals[c], attrType, attrTargetType, typeMap, zone, connection);
      }

      return new Record(compType, attributeVals);
    }

    throw createCoercionException(val.getClass(), Struct.class, val);
  }

  public static Object coerceToCustomType(final Object val, final Type sourceType, final Class<?> targetType, final Map<String, Class<?>> typeMap, final PGConnectionImpl connection) throws SQLException {

    if (val == null) {
      return null;
    }

    if (sourceType instanceof CompositeType) {

      final CompositeType compType = (CompositeType) sourceType;

      Object[] attributeVals;

      if (val instanceof Struct) {

        final Struct struct = (Struct) val;
        attributeVals = struct.getAttributes();
      } else if (val instanceof Record) {

        final Record record = (Record) val;
        attributeVals = record.getValues();
      } else {
        throw createCoercionException(val.getClass(), targetType, val);
      }

      Object dst;
      try {
        dst = targetType.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw createCoercionException(val.getClass(), targetType, val, e);
      }

      final PGSQLInputImpl in = new PGSQLInputImpl(connection, compType, typeMap, attributeVals);

      ((SQLData) dst).readSQL(in, compType.getName());

      return dst;
    }

    if (SQLData.class.isAssignableFrom(targetType)) {

      SQLData sqlData;
      try {
        sqlData = (SQLData) targetType.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw createCoercionException(val.getClass(), targetType, val, e);
      }

      // ugh, java
      final Object[] attributes = convertToObjectArray(val);

      final SQLInput inStream = new SQLInputImpl(attributes, typeMap);
      sqlData.readSQL(inStream, sourceType.getName());

      return sqlData;
    }

    throw createCoercionException(val.getClass(), targetType, val);
  }

  // Adapted from
  // http://stackoverflow.com/questions/16427319/cast-object-to-array
  private static Object[] convertToObjectArray(final Object maybeArray) {

    // if it's an array
    if (maybeArray.getClass().isArray()) {

      // check to see if it's primitive
      final Class<?> ofArray = maybeArray.getClass().getComponentType();
      if (ofArray.isPrimitive()) {

        // if so, loop through, box, and re-addd
        final ArrayList<Object> ar = new ArrayList<>();
        final int length = Array.getLength(maybeArray);
        for (int i = 0; i < length; i++) {
          ar.add(Array.get(maybeArray, i));
        }
        return ar.toArray();
      }

      // if no, just cast
      return (Object[]) maybeArray;
    }

    // just one object, make it an array of size 1
    return new Object[] { maybeArray };

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

    throw createCoercionException(val.getClass(), UUID.class, val);
  }

  public static SQLXML coerceToXML(Object val, PGConnectionImpl connection) throws SQLException {

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

    throw createCoercionException(val.getClass(), SQLXML.class, val);
  }

  public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType) {
    return new SQLException("Coercion from '" + srcType.getName() + "' to '" + dstType.getName() + "' is not supported");
  }

  public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType, Object val) {
    return new SQLException("Coercion from '" + srcType.getName() + "' to '" +
        dstType.getName() + "' is not supported (" + val + ")");
  }

  public static SQLException createCoercionException(Class<?> srcType, Class<?> dstType, Object val, Exception cause) {
    return new SQLException("Coercion from '" + srcType.getName() + "' to '" +
        dstType.getName() + "' failed (" + val + ")", cause);
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

  public static String coerceToStringFromType(Object val, Type type, Context context) throws SQLException
  {
    try {
      StringBuilder buffer = new StringBuilder();
      type.getCodec(Format.Text).encoder.encode(type, buffer, val, context);
      return buffer.toString();
    }
    catch (IOException e) {
      throw createCoercionException(val.getClass(), String.class, val);
    }
  }
}
