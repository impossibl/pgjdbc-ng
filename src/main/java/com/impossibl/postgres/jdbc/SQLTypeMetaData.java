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

import com.impossibl.postgres.api.data.ACLItem;
import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.Interval;
import com.impossibl.postgres.api.data.Range;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.Modifiers.LENGTH;
import static com.impossibl.postgres.types.Modifiers.PRECISION;
import static com.impossibl.postgres.types.Modifiers.SCALE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.sql.ResultSetMetaData.columnNullableUnknown;

/**
 * Utility functions for determine JDBC meta-data based
 * on varying amounts of information about PostgreSQL's types
 *
 * @author kdubb
 *
 */
class SQLTypeMetaData {

  static boolean requiresQuoting(Type type) {

    int sqlType = getSQLType(type);
    switch (sqlType) {
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
      default:
    }

    return true;
  }

  static boolean isCurrency(Type type) {

    return type.unwrap().getPrimitiveType() == PrimitiveType.Money;
  }

  static boolean isCaseSensitive(Type type) {

    switch (type.getCategory()) {
      case Enumeration:
      case String:
        return true;

      default:
        return false;
    }
  }

  static boolean isAutoIncrement(Type type, CompositeType relType, int relAttrNum) {

    if (relType != null && relAttrNum > 0) {

      CompositeType.Attribute attr = relType.getAttribute(relAttrNum);
      return attr != null && attr.isAutoIncrement();
    }
    else if (type instanceof DomainType) {

      return ((DomainType)type).getDefaultValue().startsWith("nextval(");
    }

    return false;
  }

  static int isNullable(Type type, CompositeType relType, int relAttrNum) {

    int nullable = isNullable(type);

    //Check the relation attribute for nullability
    if (relType != null && relAttrNum != 0) {
      CompositeType.Attribute attr = relType.getAttribute(relAttrNum);
      if (attr != null) {

        if (attr.isNullable() && nullable == columnNullableUnknown) {
          nullable = columnNullable;
        }
        else if (!attr.isNullable()) {
          nullable = columnNoNulls;
        }

      }
    }

    return nullable;
  }

  static int isNullable(Type type) {

    //Check domain types for nullability
    if (type instanceof DomainType) {
      return ((DomainType) type).isNullable() ? columnNullable : columnNoNulls;
    }

    //Everything else... we just don't know
    return columnNullableUnknown;
  }

  static boolean isSigned(Type type) {

    return type.unwrap().getCategory() == Type.Category.Numeric;
  }

  public static Type getType(Class<?> cls, Registry reg) {
    if (cls == Boolean.class) {
      return reg.loadBaseType("bool");
    }
    if (cls == Byte.class) {
      return reg.loadBaseType("int2");
    }
    if (cls == Short.class) {
      return reg.loadBaseType("int2");
    }
    if (cls == Integer.class) {
      return reg.loadBaseType("int4");
    }
    if (cls == Long.class) {
      return reg.loadBaseType("int8");
    }
    if (cls == BigInteger.class) {
      return reg.loadBaseType("numeric");
    }
    if (cls == Float.class) {
      return reg.loadBaseType("float4");
    }
    if (cls == Double.class) {
      return reg.loadBaseType("float8");
    }
    if (cls == BigDecimal.class) {
      return reg.loadBaseType("numeric");
    }
    if (cls == Character.class) {
      return reg.loadBaseType("char");
    }
    if (cls == String.class) {
      return reg.loadBaseType("text");
    }
    if (cls == Date.class) {
      return reg.loadBaseType("date");
    }
    if (cls == Time.class) {
      return reg.loadBaseType("time");
    }
    if (cls == Timestamp.class) {
      return reg.loadBaseType("timestamp");
    }
    if (cls == Array.class) {
      return reg.loadBaseType("anyarray");
    }
    return getExtendedType(cls, reg);
  }

  public static Type getExtendedType(Class<?> cls, Registry reg) {
    if (cls == Interval.class) {
      return reg.loadBaseType("interval");
    }
    else if (cls == UUID.class) {
      return reg.loadBaseType("uuid");
    }
    else if (cls == Map.class) {
      return reg.loadStableType("hstore");
    }
    else if (cls == BitSet.class) {
      return reg.loadBaseType("bits");
    }
    else if (cls == Range.class) {
      return reg.loadBaseType("range");
    }
    else if (cls == ACLItem.class) {
      return reg.loadBaseType("aclitem");
    }
    else if (cls == CidrAddr.class) {
      return reg.loadBaseType("cidr");
    }
    else if (cls == InetAddr.class) {
      return reg.loadBaseType("inet");
    }
    return null;
  }

  public static Type getType(Object val, int sqlType, Registry reg) throws SQLException {

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
        return reg.loadBaseType("text");
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
        if (val instanceof PGArray) {
          return ((PGArray) val).getType();
        }
        else if (val != null && val.getClass().isArray()) {
          Type elementType = getType(val.getClass().getComponentType(), reg);
          if (elementType != null) {
            return reg.loadType(elementType.getArrayTypeId());
          }
        }
        return null;
      case Types.ROWID:
        return reg.loadBaseType("tid");
      case Types.SQLXML:
        return reg.loadBaseType("xml");
      case Types.DISTINCT:
        return reg.loadBaseType("domain");
      case Types.STRUCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
        if (val instanceof Struct) {
          return reg.loadTransientType(((Struct) val).getSQLTypeName());
        }
        if (val instanceof SQLData) {
          return reg.loadTransientType(((SQLData) val).getSQLTypeName());
        }
        if (val != null) {
          return getExtendedType(val.getClass(), reg);
        }
        return null;
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

  static int getSQLType(Type type) {

    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      return Types.OTHER;
    }

    return primitiveToSQLTypeMatrix[ptype.ordinal()];
  }

  static String getTypeName(Type type, CompositeType relType, int relAttrNum) {

    //int4/int8 auto-increment fields -> serial/bigserial
    if (isAutoIncrement(type, relType, relAttrNum)) {

      switch (type.getPrimitiveType()) {
        case Int4:
          return "serial";

        case Int8:
          return "bigserial";

        default:
      }

    }

    return type.getName();
  }

  static int getPrecisionRadix(Type type) {

    switch (type.unwrap().getCategory()) {
      case Numeric:
        return 10;

      case BitString:
        return 2;

      default:
        return 0;
    }

  }

  static int getMaxPrecision(Type type) {

    type = type.unwrap();

    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      return 0;
    }

    switch (ptype) {
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

  static int getPrecision(Type type, int typeLength, int typeModifier) {

    type = type.unwrap();
    Map<String, Object> mods = type.getModifierParser().parse(typeModifier);

    //Lookup prec & length (if the mods have them)

    int precMod = -1;
    if (mods.containsKey(PRECISION)) {
      precMod = (int) mods.get(PRECISION);
    }

    int lenMod = -1;
    if (mods.containsKey(LENGTH)) {
      lenMod = (int) mods.get(LENGTH);
    }
    else if (typeLength != -1) {
      lenMod = typeLength;
    }
    //Calculate prec

    int prec;
    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      prec = lenMod;
    }
    else {

      switch (ptype) {
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
          if (precMod != 0) {
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

  static int getMinScale(Type type) {

    type = type.unwrap();
    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      return 0;
    }

    if (ptype == PrimitiveType.Money) {
      return 2;
    }

    return 0;
  }

  static int getMaxScale(Type type) {

    type = type.unwrap();

    PrimitiveType ptype = type.getPrimitiveType();
    if (ptype == null) {
      return 0;
    }

    if (ptype == PrimitiveType.Numeric) {
      return 1000;
    }

    return 0;
  }

  static int getScale(Type type, int typeModifier) {

    type = type.unwrap();

    Map<String, Object> mods = type.getModifierParser().parse(typeModifier);

    int scaleMod = -1;
    if (mods.get(SCALE) != null) {
      scaleMod = (int)mods.get(SCALE);
    }

    int scale = 0;

    switch (type.getPrimitiveType()) {
      case Float:
        scale = 8;
        break;

      case Double:
        scale = 17;
        break;

      case Numeric:
        scale = scaleMod;
        break;

      case Time:
      case TimeTZ:
      case Timestamp:
      case TimestampTZ:
        int precMod = -1;
        if (mods.get(PRECISION) != null) {
          precMod = (int)mods.get(PRECISION);
        }

        if (precMod == -1) {
          scale = 6;
        }
        else {
          scale = precMod;
        }
        break;

      case Interval:
        if (scaleMod == -1) {
          scale = 6;
        }
        else {
          scale = scaleMod;
        }
        break;
    }

    return scale;
  }

  static int getDisplaySize(Type type, int typeLength, int typeModifier) {

    type = type.unwrap();
    Map<String, Object> mods = type.getModifierParser().parse(typeModifier);

    int precMod = -1;
    if (mods.containsKey(PRECISION)) {
      precMod = (int) mods.get(PRECISION);
    }

    int lenMod = -1;
    if (mods.containsKey(LENGTH)) {
      lenMod = (int) mods.get(LENGTH);
    }
    else if (typeLength != -1) {
      lenMod = typeLength;
    }

    int size;

    switch (type.getCategory()) {
      case Numeric:
        if (precMod == -1) {
          size = 131089;
        }
        else {
          int prec = getPrecision(type, typeLength, typeModifier);
          int scale = getScale(type, typeModifier);
          size = prec + (scale != 0 ? 1 : 0) + 1;
        }
        break;

      case Boolean:
        size = 5; // true/false, yes/no, on/off, 1/0
        break;

      case String:
      case Enumeration:
      case BitString:
        if (lenMod == -1)
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
   * @param primType Type to determine the display size of
   * @param precision Precision modifier of type
   * @return Suggested display size
   */
  private static int calculateDateTimeDisplaySize(PrimitiveType primType, int precision) {

    if (primType == null)
      return 0;

    int size;

    switch (primType) {
      case Date:
        size = 13;
        break;

      case Time:
      case TimeTZ:
      case Timestamp:
      case TimestampTZ:

        int secondSize;
        switch (precision) {
          case -1:
            secondSize = 6 + 1;
            break;
          case 0:
            secondSize = 0;
            break;
          case 1:
            secondSize = 2 + 1;
            break;
          default:
            secondSize = precision + 1;
            break;
        }

        switch (primType) {
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
