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

import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.Modifiers.LENGTH;
import static com.impossibl.postgres.types.Modifiers.PRECISION;
import static com.impossibl.postgres.types.Modifiers.SCALE;

import java.sql.Types;
import java.util.Map;

/**
 * Utility functions for determine JDBC meta-data based
 * on varying amounts of information about PostgreSQL's types
 *
 * @author kdubb
 *
 */
class JDBCTypeMetaData {

  static boolean requiresQuoting(Type type) {

    int sqlType = JDBCTypeMapping.getSQLTypeCode(type);
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
      case Types.NULL:
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

  static boolean isSigned(Type type) {

    return type.unwrap().getCategory() == Type.Category.Numeric;
  }

  static String getTypeName(Type type, String attributeDefaultValue) {

    //int4/int8 auto-increment fields -> serial/bigserial
    if (type.isAutoIncrement() || Type.isAutoIncrement(attributeDefaultValue)) {

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
          prec = 49;
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
      scaleMod = (int) mods.get(SCALE);
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
          precMod = (int) mods.get(PRECISION);
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
