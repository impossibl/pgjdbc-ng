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

import com.impossibl.postgres.api.jdbc.PGType;
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

    int sqlType = JDBCTypeMapping.getJDBCTypeCode(type);
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

    return PGType.valueOf(type.unwrap()) == PGType.MONEY;
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

      if (PGType.valueOf(type) == PGType.INT4) {
        return "serial";
      }

      if (PGType.valueOf(type) == PGType.INT8) {
        return "bigserial";
      }

    }

    return type.getName();
  }

  static int getPrecisionRadix(Type type) {
   Type.Category c = type.unwrap().getCategory();
   if (c==null) {
            return 0;
   }
   switch (c) {
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

    PGType pgType = PGType.valueOf(type);
    if (pgType == null) {
      return 0;
    }

    switch (pgType) {
      case NUMERIC:
        return 1000;
      case TIME:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:
      case INTERVAL:
        return 6;
      case CHAR:
      case NAME:
      case BPCHAR:
      case BIT:
        return type.getLength() != null ? (int)type.getLength() : 0;
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
    PGType pgType = PGType.valueOf(type);
    if (pgType == null) {
      prec = lenMod;
    }
    else {

      switch (pgType) {
        case INT2:
          prec = 5;
          break;

        case INT4:
        case OID:
          prec = 10;
          break;

        case INT8:
        case MONEY:
          prec = 19;
          break;

        case FLOAT4:
          prec = 8;
          break;

        case FLOAT8:
          prec = 17;
          break;

        case NUMERIC:
          if (precMod != 0) {
            prec = precMod;
          }
          else {
            prec = 131072;
          }
          break;

        case DATE:
        case TIME:
        case TIME_WITH_TIMEZONE:
        case TIMESTAMP:
        case TIMESTAMP_WITH_TIMEZONE:
          prec = calculateDateTimeDisplaySize(pgType, precMod);
          break;

        case INTERVAL:
          prec = 49;
          break;

        case CHAR:
        case NAME:
        case TEXT:
        case BPCHAR:
        case VARCHAR:
        case CSTRING:
        case BIT:
        case VARBIT:
          prec = lenMod;
          break;

        case BOOL:
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

    PGType pgType = PGType.valueOf(type.unwrap());
    if (pgType == null) {
      return 0;
    }

    if (pgType == PGType.MONEY) {
      return 2;
    }

    return 0;
  }

  static int getMaxScale(Type type) {

    PGType pgType = PGType.valueOf(type.unwrap());
    if (pgType == null) {
      return 0;
    }

    if (pgType == PGType.NUMERIC) {
      return 1000;
    }

    return 0;
  }

  static int getScale(Type type, int typeModifier) {

    PGType pgType = PGType.valueOf(type.unwrap());
    if (pgType == null) {
      return 0;
    }

    Map<String, Object> mods = type.getModifierParser().parse(typeModifier);

    int scaleMod = -1;
    if (mods.get(SCALE) != null) {
      scaleMod = (int) mods.get(SCALE);
    }

    int scale = 0;

    switch (pgType) {
      case FLOAT4:
        scale = 8;
        break;

      case FLOAT8:
        scale = 17;
        break;

      case NUMERIC:
        scale = scaleMod;
        break;

      case TIME:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:
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

      case INTERVAL:
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
        size = calculateDateTimeDisplaySize(PGType.valueOf(type), precMod);
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
   * @param pgType Type to determine the display size of
   * @param precision Precision modifier of type
   * @return Suggested display size
   */
  private static int calculateDateTimeDisplaySize(PGType pgType, int precision) {

    if (pgType == null)
      return 0;

    int size;

    switch (pgType) {
      case DATE:
        size = 13;
        break;

      case TIME:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_TIMEZONE:

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

        switch (pgType) {
          case TIME:
            size = 8 + secondSize;
            break;
          case TIME_WITH_TIMEZONE:
            size = 8 + secondSize + 6;
            break;
          case TIMESTAMP:
            size = 13 + 1 + 8 + secondSize;
            break;
          case TIMESTAMP_WITH_TIMEZONE:
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
