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

import static com.impossibl.postgres.system.SystemSettings.FIELD_LENGTH_MAX;
import static com.impossibl.postgres.utils.guava.Preconditions.checkArgument;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.function.Function;

import static java.lang.Integer.min;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

abstract class BaseBinaryDecoder implements Type.Codec.Decoder<ByteBuf> {

  private Integer requiredLength;
  private boolean respectMaxLength;

  protected BaseBinaryDecoder() {
    this(null);
  }

  protected BaseBinaryDecoder(Integer requiredLength) {
    this.requiredLength = requiredLength;
  }

  protected boolean shouldRespectMaxLength() {
    return respectMaxLength;
  }

  protected void enableRespectMaxLength() {
    respectMaxLength = true;
  }

  @Override
  public Object decode(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

    targetClass = targetClass != null ? targetClass : getDefaultClass();

    if (requiredLength != null) {
      checkArgument(buffer.readableBytes() == requiredLength, "invalid length");
    }

    return decodeValue(context, type, typeLength, typeModifier, buffer, targetClass, targetContext);
  }

  protected abstract Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException;

}

interface AutoConvertingDecoder {

  interface Converter<N> {
    Object convert(Context context, N decoded, Class<?> targetClass, Object targetContext) throws IOException;
  }


  class ToStringConverter<N> implements Converter<N> {

    Function<N, String> stringConverter;

    ToStringConverter(Function<N, String> stringConverter) {
      this.stringConverter = stringConverter;
    }

    @Override
    public Object convert(Context context, N decoded, Class<?> targetClass, Object targetContext) {

      if (targetClass == String.class) {
        return stringConverter.apply(decoded);
      }

      return null;
    }

  }

}

abstract class AutoConvertingBinaryDecoder<N> extends BaseBinaryDecoder implements AutoConvertingDecoder {

  private Converter<N> converter;

  protected AutoConvertingBinaryDecoder(Integer requiredLength) {
    this(requiredLength, (Converter<N>)null);
  }

  protected AutoConvertingBinaryDecoder(Function<N, String> converter) {
    this(null, new ToStringConverter<>(converter));
  }

  protected AutoConvertingBinaryDecoder(Converter<N> converter) {
    this(null, converter);
  }

  protected AutoConvertingBinaryDecoder(Integer requiredLength, Function<N, String> converter) {
    this(requiredLength, new ToStringConverter<>(converter));
  }

  protected AutoConvertingBinaryDecoder(Integer requiredLength, Converter<N> converter) {
    super(requiredLength);
    this.converter = converter;
  }

  protected Object convertOutput(Context context, N decoded, Class<?> targetClass, Object targetContext) throws IOException {

    if (targetClass.isInstance(decoded)) {
      return targetClass.cast(decoded);
    }

    return converter.convert(context, decoded, targetClass, targetContext);
  }

  private boolean isBinary(Class<?> type) {
    return type == InputStream.class || type == byte[].class;
  }

  @Override
  protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

    // Handle binary conversions by skipping decode
    if (isBinary(targetClass)) {
      int length = buffer.readableBytes();
      if (shouldRespectMaxLength()) {
        Integer maxLength = context.getSetting(FIELD_LENGTH_MAX);
        length = maxLength != null ? min(maxLength, length) : length;
      }

      Object binaryResult = null;

      if (targetClass == InputStream.class) {
        binaryResult = new ByteBufInputStream(buffer.readRetainedSlice(length), true);
      }
      else if (targetClass == byte[].class) {
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        binaryResult = bytes;
      }

      return binaryResult;
    }

    // Decode to the native type of the decoder
    N decoded = decodeNativeValue(context, type, typeLength, typeModifier, buffer, targetClass, targetContext);

    // Attempt conversion to the requested target type
    Object converted = convertOutput(context, decoded, targetClass, targetContext);
    if (converted == null) {
      throw new ConversionException("Unable to convert value to " + targetClass);
    }

    return converted;
  }

  public abstract Class<N> getDefaultClass();

  protected abstract N decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException;

}

abstract class BaseTextDecoder implements Type.Codec.Decoder<CharSequence> {

  private boolean respectMaxLength;

  protected boolean shouldRespectMaxLength() {
    return respectMaxLength;
  }

  protected void enableRespectMaxLength() {
    respectMaxLength = true;
  }

  @Override
  public Object decode(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

    targetClass = targetClass != null ? targetClass : getDefaultClass();

    try {
      return decodeValue(context, type, typeLength, typeModifier, buffer, targetClass, targetContext);
    }
    catch (ParseException e) {
      throw new IOException(e);
    }
  }

  protected abstract Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException;

}

abstract class AutoConvertingTextDecoder<N> extends BaseTextDecoder implements AutoConvertingDecoder {

  private Converter<N> converter;

  protected AutoConvertingTextDecoder(Function<N, String> converter) {
    this(new ToStringConverter<>(converter));
  }

  protected AutoConvertingTextDecoder(Converter<N> converter) {
    this.converter = converter;
  }

  protected Object convertOutput(Context context, N decoded, Class<?> targetClass, Object targetContext) throws IOException {

    if (targetClass.isInstance(decoded)) {
      return targetClass.cast(decoded);
    }

    return converter.convert(context, decoded, targetClass, targetContext);
  }

  private boolean isBinary(Class<?> type) {
    return type == InputStream.class || type == byte[].class;
  }

  @Override
  protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

    if (isBinary(targetClass)) {
      int length = buffer.length();
      if (shouldRespectMaxLength()) {
        Integer maxLength = context.getSetting(FIELD_LENGTH_MAX);
        length = maxLength != null ? min(maxLength, length) : length;
      }

      byte[] bytes = buffer.subSequence(0, length).toString().getBytes(context.getCharset());

      Object binaryResult = null;

      if (targetClass == InputStream.class) {
        binaryResult = new ByteArrayInputStream(bytes);
      }
      else if (targetClass == byte[].class) {
        binaryResult = bytes;
      }

      return binaryResult;
    }

    // Decode to the native type of the decoder
    N decoded = decodeNativeValue(context, type, typeLength, typeModifier, buffer, targetClass, targetContext);

    // Attempt conversion to the requested target type
    Object converted = convertOutput(context, decoded, targetClass, targetContext);
    if (converted == null) {
      throw new ConversionException("Unable to convert value to " + targetClass);
    }

    return converted;
  }

  public abstract Class<N> getDefaultClass();

  protected abstract N decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException;

}
