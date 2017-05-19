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
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.FIELD_MONEY_FRACTIONAL_DIGITS;
import static com.impossibl.postgres.types.PrimitiveType.Money;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;

import static java.math.RoundingMode.HALF_UP;

import io.netty.buffer.ByteBuf;

public class Moneys extends SimpleProcProvider {

  public Moneys() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "cash_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Money;
    }

    @Override
    public Class<?> getOutputType() {
      return BigDecimal.class;
    }

    @Override
    public BigDecimal decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 8) {
        throw new IOException("invalid length");
      }

      long val = buffer.readLong();

      int fracDigits = getFractionalDigits(context);

      return new BigDecimal(BigInteger.valueOf(val), fracDigits);
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return BigDecimal.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Money;
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        int fracDigits = getFractionalDigits(context);

        BigDecimal dec = (BigDecimal) val;

        dec = dec.setScale(fracDigits, HALF_UP);

        buffer.writeInt(8);
        buffer.writeLong(dec.unscaledValue().longValue());
      }

    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Money;
    }

    @Override
    public Class<?> getOutputType() {
      return BigDecimal.class;
    }

    @Override
    public BigDecimal decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      try {
        return (BigDecimal) context.getCurrencyFormatter().parse(buffer.toString());
      }
      catch (ParseException e) {
        throw new IOException(e);
      }
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return BigDecimal.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Money;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      buffer.append(context.getCurrencyFormatter().format(val));
    }

  }

  static int getFractionalDigits(Context context) {

    Object val = context.getSetting(FIELD_MONEY_FRACTIONAL_DIGITS);
    if (val == null)
      return 2;

    return (Integer)val;
  }

}
