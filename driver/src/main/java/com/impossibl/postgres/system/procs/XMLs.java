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

import com.impossibl.postgres.jdbc.PGDirectConnection;
import com.impossibl.postgres.jdbc.PGSQLXML;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.sql.SQLXML;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;


/*
 * XML codec
 *
 */
public class XMLs extends SimpleProcProvider {

  public XMLs() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "xml_");
  }

  static byte[] convertInput(Context context, Type type, Object value) throws ConversionException {

    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return ((String) value).getBytes(context.getCharset());
    }

    if (value instanceof PGSQLXML) {
      return ((PGSQLXML) value).getData();
    }

    throw new ConversionException(value.getClass(), type);
  }

  static Object convertOutput(Context context, Type type, byte[] data, Class<?> targetClass) throws ConversionException {

    if (targetClass == SQLXML.class) {
      return new PGSQLXML((PGDirectConnection) context.unwrap(), data);
    }
    else if (targetClass == String.class) {
      return new String(data, context.getCharset());
    }
    else if (targetClass == byte[].class) {
      return data;
    }

    throw new ConversionException(type, targetClass);
  }

  static class BinDecoder extends ConvertedBytes.BinDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return SQLXML.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      byte[] data = (byte[]) super.decodeValue(context, type, typeLength, typeModifier, buffer, targetClass, targetContext);

      return convertOutput(context, type, data, targetClass);
    }

  }

  static class BinEncoder extends ConvertedBytes.BinEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      value = convertInput(context, type, value);
      if (value == null) {
        value = new byte[0];
      }

      super.encodeValue(context, type, value, sourceContext, buffer);
    }
  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return SQLXML.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      byte[] data = buffer.toString().getBytes(context.getCharset());

      return convertOutput(context, type, data, targetClass);
    }

  }

  static class TxtEncoder extends Strings.TxtEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      byte[] bytes = convertInput(context, type, value);
      if (bytes == null) {
        bytes = new byte[0];
      }

      String text = new String(bytes, context.getCharset());

      super.encodeValue(context, type, text, sourceContext, buffer);
    }

  }

}
