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
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.SystemSettings.MONEY_FRACTIONAL_DIGITS;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_UP;

import io.netty.buffer.ByteBuf;

public class Moneys extends SimpleProcProvider {

  public Moneys() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "cash_");
  }

  private static BigDecimal convertStringInput(Context context, String value) throws ConversionException {
    try {
      return (BigDecimal) context.getClientDecimalFormatter().parse(value);
    }
    catch (ParseException e) {
      throw new ConversionException("Invalid Long", e);
    }
  }

  private static String convertStringOutput(Context context, Number number) {
    return context.getClientDecimalFormatter().format(number);
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<BigDecimal> {

    BinDecoder() {
      super(8);
    }

    @Override
    public Class<BigDecimal> getDefaultClass() {
      return BigDecimal.class;
    }

    @Override
    protected BigDecimal decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      long val = buffer.readLong();

      int fracDigits = context.getSetting(MONEY_FRACTIONAL_DIGITS);

      return new BigDecimal(BigInteger.valueOf(val), fracDigits);
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<BigDecimal> {

    BinEncoder() {
      super(8, new NumericEncodingConverter<>(Moneys::convertStringInput, val -> val ? ONE : ZERO, val -> BigDecimal.valueOf(val.doubleValue())));
    }

    @Override
    public Class<BigDecimal> getDefaultClass() {
      return BigDecimal.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, BigDecimal value, Object sourceContext, ByteBuf buffer) throws IOException {

      int fracDigits = context.getSetting(MONEY_FRACTIONAL_DIGITS);

      value = value.setScale(fracDigits, HALF_UP);

      buffer.writeLong(value.unscaledValue().longValue());
    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<BigDecimal> {

    TxtDecoder() {
      super(new NumericDecodingConverter<>(Moneys::convertStringOutput));
    }

    @Override
    public Class<BigDecimal> getDefaultClass() {
      return BigDecimal.class;
    }

    @Override
    protected BigDecimal decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {
      try {
        return (BigDecimal) context.getServerCurrencyFormatter().parse(buffer.toString());
      }
      catch (ParseException e) {
        throw new IOException(e);
      }
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<BigDecimal> {

    protected TxtEncoder() {
      super(new NumericEncodingConverter<>(Moneys::convertStringInput, val -> val ? ONE : ZERO, val -> BigDecimal.valueOf(val.doubleValue())));
    }

    @Override
    public Class<BigDecimal> getDefaultClass() {
      return BigDecimal.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, BigDecimal value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(context.getServerCurrencyFormatter().format(value));
    }

  }

}
