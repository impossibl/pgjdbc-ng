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
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;


abstract class NumericBinaryDecoder<N extends Number> extends AutoConvertingBinaryDecoder<N> {

  protected NumericBinaryDecoder(Integer requiredLength, ContextConversionFunction<N, String> converter) {
    super(requiredLength, new NumericDecodingConverter<>(converter));
  }

}

abstract class NumericTextDecoder<N extends Number> extends AutoConvertingTextDecoder<N> {

  protected NumericTextDecoder(ContextConversionFunction<N, String> converter) {
    super(new NumericDecodingConverter<>(converter));
  }

}

class NumericDecodingConverter<N extends Number> implements AutoConvertingDecoder.Converter<N> {

  private ContextConversionFunction<N, String> stringConverter;

  NumericDecodingConverter(ContextConversionFunction<N, String> stringConverter) {
    this.stringConverter = stringConverter;
  }

  @Override
  public Object convert(Context context, N decoded, Class<?> targetClass, Object targetContext) throws ConversionException {

    if (targetClass == String.class) {
      return stringConverter.apply(context, decoded);
    }

    if (targetClass == BigDecimal.class) {

      BigDecimal decimal;

      if (decoded instanceof Byte || decoded instanceof Short || decoded instanceof Integer || decoded instanceof Long) {
        decimal = BigDecimal.valueOf(decoded.longValue());
      }
      else if (decoded instanceof Float) {
        decimal = BigDecimal.valueOf(decoded.floatValue());
      }
      else if (decoded instanceof Double) {
        decimal = BigDecimal.valueOf(decoded.doubleValue());
      }
      else if (decoded instanceof BigInteger) {
        decimal = new BigDecimal(decoded.toString());
      }
      else {
        return null;
      }

      if (targetContext != null) {
        int scale = ((Number) targetContext).intValue();
        decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
      }

      return decimal;
    }

    if (targetClass == Double.class || targetClass == double.class) {
      return decoded.doubleValue();
    }

    if (targetClass == Float.class || targetClass == float.class) {
      return decoded.floatValue();
    }

    if (targetClass == BigInteger.class) {
      if (decoded instanceof BigDecimal) {
        return ((BigDecimal) decoded).toBigInteger();
      }
      return BigInteger.valueOf(decoded.longValue());
    }

    if (targetClass == Boolean.class || targetClass == boolean.class) {
      return decoded.byteValue() != 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    if (targetClass == Byte.class || targetClass == byte.class) {
      long longVal = decoded.longValue();
      if (longVal < Byte.MIN_VALUE || longVal > Byte.MAX_VALUE) {
        throw new ArithmeticException("Value out of byte range");
      }
      return (byte) longVal;
    }

    if (targetClass == Short.class || targetClass == short.class) {
      long longVal = decoded.longValue();
      if (longVal < Short.MIN_VALUE || longVal > Short.MAX_VALUE) {
        throw new ArithmeticException("Value out of short range");
      }
      return (short) longVal;
    }

    if (targetClass == Integer.class || targetClass == int.class) {
      long longVal = decoded.longValue();
      if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
        throw new ArithmeticException("Value out of int range");
      }
      return (int) longVal;
    }

    if (targetClass == Long.class || targetClass == long.class) {
      if (decoded instanceof BigDecimal) {
        return ((BigDecimal) decoded).toBigInteger().longValueExact();
      }
      else if (decoded instanceof BigInteger) {
        return ((BigInteger) decoded).longValueExact();
      }
      return decoded.longValue();
    }

    return null;
  }

}
