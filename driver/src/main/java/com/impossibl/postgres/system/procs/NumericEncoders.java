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

interface ConversionFunction<T, R> {

  R apply(T t) throws ConversionException;

}

interface ContextConversionFunction<T, R> {

  R apply(Context context, T t) throws ConversionException;

}

abstract class NumericBinaryEncoder<N extends Number> extends AutoConvertingBinaryEncoder<N> {

  protected NumericBinaryEncoder(Integer binaryLength, ContextConversionFunction<String, N> stringCast, ConversionFunction<Boolean, N> boolCast, ConversionFunction<Number, N> numberCast) {
    super(binaryLength, new NumericEncodingConverter<>(stringCast, boolCast, numberCast));
  }

}

abstract class NumericTextEncoder<N extends Number> extends AutoConvertingTextEncoder<N> {

  protected NumericTextEncoder(ContextConversionFunction<String, N> stringCast, ConversionFunction<Boolean, N> boolCast, ConversionFunction<Number, N> numberCast) {
    super(new NumericEncodingConverter<>(stringCast, boolCast, numberCast));
  }

}

class NumericEncodingConverter<N extends Number> implements AutoConvertingEncoder.Converter<N> {

  private ContextConversionFunction<String, N> stringCast;
  private ConversionFunction<Boolean, N> boolCast;
  private ConversionFunction<Number, N> numberCast;

  NumericEncodingConverter(ContextConversionFunction<String, N> stringCast, ConversionFunction<Boolean, N> boolCast, ConversionFunction<Number, N> numberCast) {
    this.stringCast = stringCast;
    this.boolCast = boolCast;
    this.numberCast = numberCast;
  }

  @Override
  public N convert(Context context, Object source, Object sourceContext) throws ConversionException {

    if (source instanceof String) {
      return stringCast.apply(context, (String) source);
    }

    if (source instanceof Boolean) {
      return boolCast.apply((Boolean) source);
    }

    if (source instanceof Number) {
      return numberCast.apply((Number) source);
    }

    return null;
  }

}
