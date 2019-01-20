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
import com.impossibl.postgres.system.JavaTypeMapping;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import static com.impossibl.postgres.system.SystemSettings.FIELD_LENGTH_MAX;
import static com.impossibl.postgres.types.Modifiers.LENGTH;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class Strings extends SimpleProcProvider {

  static final BinDecoder BINARY_DECODER = new BinDecoder();
  static final BinEncoder BINARY_ENCODER = new BinEncoder();
  static final TxtDecoder TEXT_DECODER = new TxtDecoder();
  static final TxtEncoder TEXT_ENCODER = new TxtEncoder();

  public Strings() {
    super(TEXT_ENCODER, TEXT_DECODER, BINARY_ENCODER, BINARY_DECODER, new ModParser(),
        "text", "varchar", "bpchar", "char", "enum_", "json_", "cstring_", "citext", "unknown");
  }

  private static Bools.TxtDecoder boolDecoder = new Bools.TxtDecoder();
  private static Bools.TxtEncoder boolEncoder = new Bools.TxtEncoder();
  private static Int2s.TxtDecoder shortDecoder = new Int2s.TxtDecoder();
  private static Int2s.TxtEncoder shortEncoder = new Int2s.TxtEncoder();
  private static Int4s.TxtDecoder intDecoder = new Int4s.TxtDecoder();
  private static Int4s.TxtEncoder intEncoder = new Int4s.TxtEncoder();
  private static Int8s.TxtDecoder longDecoder = new Int8s.TxtDecoder();
  private static Int8s.TxtEncoder longEncoder = new Int8s.TxtEncoder();
  private static Float4s.TxtDecoder floatDecoder = new Float4s.TxtDecoder();
  private static Float4s.TxtEncoder floatEncoder = new Float4s.TxtEncoder();
  private static Float8s.TxtDecoder doubleDecoder = new Float8s.TxtDecoder();
  private static Float8s.TxtEncoder doubleEncoder = new Float8s.TxtEncoder();
  private static Numerics.TxtDecoder decimalDecoder = new Numerics.TxtDecoder();
  private static Numerics.TxtEncoder decimalEncoder = new Numerics.TxtEncoder();
  private static Bytes.TxtEncoder bytesEncoder = new Bytes.TxtEncoder();

  private static String convertInput(Context context, Object value, Long length) throws IOException {

    if (value instanceof CharSequence) {
      return value.toString();
    }

    if (value instanceof InputStream) {
      InputStream stream = (InputStream) value;
      if (length != null) {
        stream = ByteStreams.limit(stream, length);
      }
      return CharStreams.toString(new InputStreamReader(stream, context.getCharset()));
    }

    if (value instanceof Reader) {
      Reader reader = (Reader) value;
      if (length != null) {
        reader = CharStreams.limit(reader, length);
      }
      return CharStreams.toString(reader);
    }

    StringBuilder out = new StringBuilder();

    if (value instanceof Character) {
      out.append(value);
    }
    else if (value instanceof Boolean) {
      boolEncoder.encodeNativeValue(context, null, (Boolean) value, null, out);
    }
    else if (value instanceof Short) {
      shortEncoder.encodeNativeValue(context, null, (Short) value, null, out);
    }
    else if (value instanceof Integer) {
      intEncoder.encodeNativeValue(context, null, (Integer) value, null, out);
    }
    else if (value instanceof Long) {
      longEncoder.encodeNativeValue(context, null, (Long) value, null, out);
    }
    else if (value instanceof Float) {
      floatEncoder.encodeNativeValue(context, null, (Float) value, null, out);
    }
    else if (value instanceof Double) {
      doubleEncoder.encodeNativeValue(context, null, (Double) value, null, out);
    }
    else if (value instanceof BigDecimal) {
      decimalEncoder.encodeNativeValue(context, null, (BigDecimal) value, null, out);
    }
    else if (value instanceof byte[]) {
      bytesEncoder.encodeValue(context, null, value, null, out);
    }
    else {
      out.append(value);
    }

    return out.toString();
  }

  private static Object convertOutput(Context context, String decoded, Class<?> targetClass, Object targetContext) throws IOException {

    try {

      if (targetClass == String.class) {
        return decoded;
      }

      Type type = JavaTypeMapping.getType(targetClass, context.getRegistry());
      if (type != null && type.getTextCodec() != null) {
        return type.getTextCodec().getDecoder().decode(context, type, null, null, decoded, targetClass, targetContext);
      }

      if (targetClass == Boolean.class || targetClass == boolean.class) {
        return boolDecoder.decodeNativeValue(context, null, null, null, decoded, Boolean.class, targetContext);
      }

      if (targetClass == Byte.class || targetClass == byte.class) {
        return Byte.valueOf(decoded);
      }

      if (targetClass == Short.class || targetClass == short.class) {
        return shortDecoder.decodeNativeValue(context, null, null, null, decoded, Short.class, targetContext);
      }

      if (targetClass == Integer.class || targetClass == int.class) {
        return intDecoder.decodeNativeValue(context, null, null, null, decoded, Integer.class, targetContext);
      }

      if (targetClass == Long.class || targetClass == long.class) {
        return longDecoder.decodeNativeValue(context, null, null, null, decoded, Long.class, targetContext);
      }

      if (targetClass == BigInteger.class) {
        return new BigInteger(decoded);
      }

      if (targetClass == Float.class || targetClass == float.class) {
        return floatDecoder.decodeNativeValue(context, null, null, null, decoded, Float.class, targetContext);
      }

      if (targetClass == Double.class || targetClass == double.class) {
        return doubleDecoder.decodeNativeValue(context, null, null, null, decoded, Double.class, targetContext);
      }

      if (targetClass == BigDecimal.class) {
        return decimalDecoder.decodeNativeValue(context, null, null, null, decoded, BigDecimal.class, targetContext);
      }

      if (targetClass == URL.class) {
        return new URL(decoded);
      }

      return null;
    }
    catch (ParseException e) {
      throw new IOException(e);
    }
  }

  public static class BinDecoder extends AutoConvertingBinaryDecoder<String> {

    public BinDecoder() {
      super(null, Strings::convertOutput);
      enableRespectMaxLength();
    }

    @Override
    public Class<String> getDefaultClass() {
      return String.class;
    }

    @Override
    protected String decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      byte[] bytes = new byte[length];

      buffer.readBytes(bytes);
      buffer.skipBytes(length - bytes.length);

      CharSequence value = new String(bytes, context.getCharset());
      Integer maxLength = context.getSetting(FIELD_LENGTH_MAX);
      if (maxLength != null) {
        value = value.subSequence(0, maxLength);
      }

      return value.toString();
    }

  }

  public static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object val, Object sourceContext, ByteBuf buffer) throws IOException {

      Long specifiedLength = (Long) sourceContext;
      String value = convertInput(context, val, specifiedLength);

      buffer.writeCharSequence(value, context.getCharset());
    }

  }

  public static class TxtDecoder extends AutoConvertingTextDecoder<String> {

    TxtDecoder() {
      super(Strings::convertOutput);
      enableRespectMaxLength();
    }

    @Override
    public Class<String> getDefaultClass() {
      return String.class;
    }

    @Override
    protected String decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      CharSequence value = buffer;

      Integer maxLength = context.getSetting(FIELD_LENGTH_MAX);
      if (maxLength != null) {
        value = value.subSequence(0, maxLength);
      }

      return value.toString();
    }

  }

  public static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object val, Object sourceContext, StringBuilder buffer) throws IOException {

      Long specifiedLength = (Long) sourceContext;
      String value = convertInput(context, val, specifiedLength);

      buffer.append(value);
    }

  }

  private static class ModParser implements Modifiers.Parser {

    @Override
    public Map<String, Object> parse(long mod) {

      Map<String, Object> mods = new HashMap<>();

      if (mod > 4) {
        mods.put(LENGTH, (int) (mod - 4));
      }

      return mods;
    }

  }

}
