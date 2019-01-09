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

import static com.impossibl.postgres.utils.guava.Preconditions.checkNotNull;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

abstract class BaseBinaryEncoder implements Type.Codec.Encoder<ByteBuf> {

  private Integer binaryLength;

  protected BaseBinaryEncoder() {
    this(null);
  }

  protected BaseBinaryEncoder(Integer binaryLength) {
    this.binaryLength = binaryLength;
  }

  @Override
  public void encode(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {
    checkNotNull(value);

    int start = buffer.writerIndex();

    encodeValue(context, type, value, sourceContext, buffer);

    if (binaryLength != null && binaryLength != (buffer.writerIndex() - start)) {
      throw new IOException("Invalid encoded length");
    }

  }

  protected abstract void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException;

}

interface AutoConvertingEncoder {

  interface Converter<N> {
    N convert(Context context, Object source, Object sourceContext) throws ConversionException;
  }

  interface StringConverter<N> {
    N convert(String value) throws ConversionException;
  }

  class FromStringConverter<N> implements Converter<N> {

    StringConverter<N> converter;

    FromStringConverter(StringConverter<N> converter) {
      this.converter = converter;
    }

    @Override
    public N convert(Context context, Object source, Object sourceContext) throws ConversionException {

      if (source instanceof String) {
        return converter.convert((String) source);
      }

      return null;
    }
  }

}

abstract class AutoConvertingBinaryEncoder<N> extends BaseBinaryEncoder implements AutoConvertingEncoder {

  private Converter<N> converter;

  protected AutoConvertingBinaryEncoder(StringConverter<N> converter) {
    this(null, new FromStringConverter<>(converter));
  }

  protected AutoConvertingBinaryEncoder(Integer binaryLength, StringConverter<N> converter) {
    this(binaryLength, new FromStringConverter<>(converter));
  }

  protected AutoConvertingBinaryEncoder(Converter<N> converter) {
    this(null, converter);
  }

  protected AutoConvertingBinaryEncoder(Integer binaryLength, Converter<N> converter) {
    super(binaryLength);
    this.converter = converter;
  }

  protected N convertInput(Context context, Type type, Object source, Object sourceContext) throws ConversionException {

    if (getDefaultClass().isInstance(source)) {
      return getDefaultClass().cast(source);
    }
    if (converter != null) {
      return converter.convert(context, source, sourceContext);
    }

    throw new ConversionException(source.getClass(), type);
  }

  @Override
  protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

    N convertedValue = convertInput(context, type, value, sourceContext);
    if (convertedValue == null) {
      throw new IOException("Error coercing value");
    }

    encodeNativeValue(context, type, convertedValue, sourceContext, buffer);
  }

  protected abstract Class<N> getDefaultClass();

  protected abstract void encodeNativeValue(Context context, Type type, N value, Object sourceContext, ByteBuf buffer) throws IOException;

}



abstract class BaseTextEncoder implements Type.Codec.Encoder<StringBuilder> {

  protected BaseTextEncoder() {
  }

  @Override
  public void encode(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

    if (value == null) {
      return;
    }

    encodeValue(context, type, value, sourceContext, buffer);
  }

  protected abstract void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException;

}


abstract class AutoConvertingTextEncoder<N> extends BaseTextEncoder implements AutoConvertingEncoder {

  private Converter<N> converter;

  protected AutoConvertingTextEncoder(StringConverter<N> converter) {
    this(new FromStringConverter<>(converter));
  }

  protected AutoConvertingTextEncoder(Converter<N> converter) {
    this.converter = converter;
  }

  protected N convertInput(Context context, Type type, Object source, Object sourceContext) throws ConversionException {

    if (getDefaultClass().isInstance(source)) {
      return getDefaultClass().cast(source);
    }
    if (converter != null) {
      return converter.convert(context, source, sourceContext);
    }

    throw new ConversionException(source.getClass(), type);
  }

  @Override
  protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

    N convertedValue = convertInput(context, type, value, sourceContext);
    if (convertedValue == null) {
      throw new IOException("Error coercing value");
    }

    encodeNativeValue(context, type, convertedValue, sourceContext, buffer);
  }

  protected abstract Class<N> getDefaultClass();

  protected abstract void encodeNativeValue(Context context, Type type, N value, Object sourceContext, StringBuilder buffer) throws IOException;

}
