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

import com.impossibl.postgres.api.jdbc.PGAnyType;
import com.impossibl.postgres.system.JavaTypeMapping;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Ref;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

class JDBCTypeMapping {

  public static Type getType(SQLType sqlType, Object value, Registry reg) throws SQLException {

    if (sqlType instanceof JDBCType) {

      return JDBCTypeMapping.getType(sqlType.getVendorTypeNumber(), value, reg);
    }
    else if (sqlType instanceof PGAnyType) {

      try {
        if (sqlType.getVendorTypeNumber() != null) {

          return reg.loadType(sqlType.getVendorTypeNumber());
        }
        else {
          return reg.loadStableType(sqlType.getName());
        }
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }
    }

    throw new PGSQLSimpleException("Unsupported SQLType");
  }

  static int getSQLTypeCode(Class<?> cls) {
    if (cls == Boolean.class) {
      return Types.BOOLEAN;
    }
    if (cls == Byte.class) {
      return Types.TINYINT;
    }
    if (cls == Short.class) {
      return Types.SMALLINT;
    }
    if (cls == Integer.class) {
      return Types.INTEGER;
    }
    if (cls == Long.class) {
      return Types.BIGINT;
    }
    if (cls == BigInteger.class) {
      return Types.BIGINT;
    }
    if (cls == Float.class) {
      return Types.REAL;
    }
    if (cls == Double.class) {
      return Types.DOUBLE;
    }
    if (cls == BigDecimal.class) {
      return Types.DECIMAL;
    }
    if (Number.class.isAssignableFrom(cls)) {
      return Types.NUMERIC;
    }
    if (cls == Character.class) {
      return Types.CHAR;
    }
    if (cls == String.class) {
      return Types.VARCHAR;
    }
    if (cls == Date.class) {
      return Types.DATE;
    }
    if (cls == Time.class) {
      return Types.TIME;
    }
    if (cls == Timestamp.class) {
      return Types.TIMESTAMP;
    }
    if (cls == byte[].class) {
      return Types.VARBINARY;
    }
    if (InputStream.class.isAssignableFrom(cls)) {
      return Types.LONGVARBINARY;
    }
    if (Reader.class.isAssignableFrom(cls)) {
      return Types.LONGNVARCHAR;
    }
    if (Blob.class.isAssignableFrom(cls)) {
      return Types.BLOB;
    }
    if (Clob.class.isAssignableFrom(cls)) {
      return Types.CLOB;
    }
    if (Array.class.isAssignableFrom(cls)) {
      return Types.ARRAY;
    }
    if (Struct.class.isAssignableFrom(cls)) {
      return Types.STRUCT;
    }
    if (SQLData.class.isAssignableFrom(cls)) {
      return Types.STRUCT;
    }
    if (Ref.class.isAssignableFrom(cls)) {
      return Types.REF;
    }
    if (SQLXML.class.isAssignableFrom(cls)) {
      return Types.SQLXML;
    }
    return Types.OTHER;
  }

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
    primitiveToSQLTypeMatrix[PrimitiveType.TimeTZ.ordinal()] = Types.TIME_WITH_TIMEZONE;
    primitiveToSQLTypeMatrix[PrimitiveType.Timestamp.ordinal()] = Types.TIMESTAMP;
    primitiveToSQLTypeMatrix[PrimitiveType.TimestampTZ.ordinal()] = Types.TIMESTAMP_WITH_TIMEZONE;
    primitiveToSQLTypeMatrix[PrimitiveType.Oid.ordinal()] = Types.INTEGER;
    primitiveToSQLTypeMatrix[PrimitiveType.Tid.ordinal()] = Types.ROWID;
    primitiveToSQLTypeMatrix[PrimitiveType.Array.ordinal()] = Types.ARRAY;
    primitiveToSQLTypeMatrix[PrimitiveType.Record.ordinal()] = Types.STRUCT;
    primitiveToSQLTypeMatrix[PrimitiveType.Domain.ordinal()] = Types.DISTINCT;
    primitiveToSQLTypeMatrix[PrimitiveType.Binary.ordinal()] = Types.BINARY;
    primitiveToSQLTypeMatrix[PrimitiveType.Bits.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Range.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.UUID.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Interval.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Unknown.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Point.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Box.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.LineSegment.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Line.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Path.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Polygon.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Circle.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Inet.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.Cidr.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.MacAddr.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.HStore.ordinal()] = Types.OTHER;
    primitiveToSQLTypeMatrix[PrimitiveType.ACLItem.ordinal()] = Types.OTHER;
  }

  static int getSQLTypeCode(Type type) {

    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      return Types.OTHER;
    }

    return primitiveToSQLTypeMatrix[ptype.ordinal()];
  }

  static Type getType(int sqlType, Object val, Registry reg) throws SQLException {
    switch (sqlType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return reg.loadBaseType("bool");
      case Types.TINYINT:
      case Types.SMALLINT:
        return reg.loadBaseType("int2");
      case Types.INTEGER:
        return reg.loadBaseType("int4");
      case Types.BIGINT:
        return reg.loadBaseType("int8");
      case Types.REAL:
        return reg.loadBaseType("float4");
      case Types.FLOAT:
      case Types.DOUBLE:
        return reg.loadBaseType("float8");
      case Types.NUMERIC:
      case Types.DECIMAL:
        return reg.loadBaseType("numeric");
      case Types.CHAR:
        return reg.loadBaseType("char");
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return reg.loadBaseType("varchar");
      case Types.DATE:
        return reg.loadBaseType("date");
      case Types.TIME:
        return reg.loadBaseType("time");
      case Types.TIME_WITH_TIMEZONE:
        return reg.loadBaseType("timetz");
      case Types.TIMESTAMP:
        return reg.loadBaseType("timestamp");
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return reg.loadBaseType("timestamptz");
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return reg.loadBaseType("bytea");
      case Types.BLOB:
      case Types.CLOB:
        return reg.loadBaseType("oid");
      case Types.ARRAY:
        try {
          if (val instanceof PGArray) {
            return ((PGArray) val).getType();
          }
          else if (val != null) {
            Type elementType;
            if (java.lang.reflect.Array.getLength(val) > 0) {
              Object element = java.lang.reflect.Array.get(val, 0);
              elementType = getType(getSQLTypeCode(element.getClass()), element, reg);
            }
            else {
              elementType = JavaTypeMapping.getType(val.getClass().getComponentType(), reg);
            }
            if (elementType == null) {
              return null;
            }
            // Now that we have the most accurate element type we
            // can determine, use that to find its actual array type.
            return reg.loadType(elementType.getArrayTypeId());
          }
          return null;
        }
        catch (IOException e) {
          throw makeSQLException(e);
        }
      case Types.ROWID:
        return reg.loadBaseType("tid");
      case Types.SQLXML:
        return reg.loadBaseType("xml");
      case Types.DISTINCT:
        return reg.loadBaseType("domain");
      case Types.STRUCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
        try {
          if (val instanceof Struct) {
            return reg.loadTransientType(((Struct) val).getSQLTypeName());
          }
          if (val instanceof SQLData) {
            return reg.loadTransientType(((SQLData) val).getSQLTypeName());
          }
          if (val != null) {
            return JavaTypeMapping.getExtendedType(val.getClass(), reg);
          }
          return null;
        }
        catch (IOException e) {
          throw makeSQLException(e);
        }
      case Types.REF:
      case Types.DATALINK:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NCLOB:
      default:
        return null;
    }

  }

}
