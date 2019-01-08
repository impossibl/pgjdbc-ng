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

import com.impossibl.postgres.jdbc.PGBlob;
import com.impossibl.postgres.jdbc.PGBufferBlob;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.ByteStreams;

import static com.impossibl.postgres.system.SystemSettings.FIELD_LENGTH_MAX;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static java.lang.Math.min;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

public class Bytes extends SimpleProcProvider {

  public Bytes() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "bytea");
  }

  private static byte[] coerceInput(Type type, Object source, Long length) throws IOException {

    if (source instanceof byte[]) {
      return (byte[]) source;
    }

    if (source instanceof InputStream) {
      InputStream is = (InputStream) source;
      if (length != null) {
        is = ByteStreams.limit(is, length);
      }
      return ByteStreams.toByteArray(is);
    }

    if (source instanceof PGBlob) {
      PGBlob blob = (PGBlob) source;
      try (InputStream in = blob.getBinaryStream()) {
        return ByteStreams.toByteArray(in);
      }
      catch (SQLException e) {
        throw new IOException("Error loading blob data", e);
      }
    }

    throw new ConversionException(source.getClass(), type);
  }

  private static Object convertOutput(Context context, Type type, ByteBuf decoded, Class<?> targetClass) throws ConversionException {

    if (targetClass == InputStream.class) {
      return new ByteBufInputStream(decoded, true);
    }

    if (targetClass == byte[].class) {
      byte[] bytes = new byte[decoded.readableBytes()];
      decoded.readBytes(bytes);
      decoded.release();
      return bytes;
    }

    if (targetClass == String.class) {
      byte[] bytes = new byte[decoded.readableBytes()];
      decoded.readBytes(bytes);
      decoded.release();
      return new String(bytes, context.getCharset());
    }

    if (targetClass == Blob.class) {
      return new PGBufferBlob(decoded.retainedDuplicate());
    }

    throw new ConversionException(type, targetClass);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      enableRespectMaxLength();
    }

    @Override
    public Class<?> getDefaultClass() {
      return InputStream.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      int readLength;
      Integer maxLength = context.getSetting(FIELD_LENGTH_MAX);
      if (maxLength != null) {
        readLength = min(maxLength, length);
      }
      else {
        readLength = length;
      }

      ByteBuf bytes = buffer.readRetainedSlice(readLength);
      try {

        buffer.skipBytes(length - readLength);

        return convertOutput(context, type, bytes, targetClass);
      }
      catch (Throwable t) {
        bytes.release();
        throw t;
      }
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object val, Object sourceContext, ByteBuf buffer) throws IOException {

      Long specifiedLength = (Long) sourceContext;
      byte[] value = Bytes.coerceInput(type, val, specifiedLength);

      if (specifiedLength != null && specifiedLength != value.length) {
        throw new IOException("Invalid binary length");
      }

      buffer.writeBytes(value);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    TxtDecoder() {
      enableRespectMaxLength();
    }

    @Override
    public Class<?> getDefaultClass() {
      return InputStream.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      ByteBuf bytes;

      if (buffer.length() > 2 && buffer.charAt(0) == '\\' && buffer.charAt(1) == 'x') {
        bytes = Unpooled.wrappedBuffer(decodeHex(buffer.subSequence(2, buffer.length())));
      }
      else {
        bytes = decodeEscape(buffer);
      }

      try {
        return convertOutput(context, type, bytes, targetClass);
      }
      catch (Throwable t) {
        bytes.release();
        throw t;
      }
    }

    ByteBuf decodeEscape(CharSequence buffer) {

      int length = buffer.length();
      ByteBuf data = Unpooled.buffer(length);
      try {

        for (int i = 0; i < length; ++i) {

          char ch = buffer.charAt(i);
          if (ch == '\\') {

            char ch1 = buffer.charAt(++i);
            if (ch1 == '\\') {
              data.writeByte((byte) '\\');
            }
            if (i == length - 1) break;

            char ch2 = buffer.charAt(++i);
            char ch3 = buffer.charAt(++i);
            data.writeByte((byte) (ch1 * 64 + ch2 * 8 + ch3));
          }

          data.writeByte((byte) ch);
        }

        return data.capacity(data.writerIndex());
      }
      catch (Throwable t) {
        data.release();
        throw t;
      }

    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object val, Object sourceContext, StringBuilder buffer) throws IOException {

      Long specifiedLength = (Long) sourceContext;
      byte[] value = coerceInput(type, val, specifiedLength);

      if (specifiedLength != null && specifiedLength != value.length) {
        throw new IOException("Mismatch in length of binary arguments");
      }

      buffer.append("\\x");

      encodeHex(value, buffer);

    }

  }

  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  public static byte[] decodeHex(CharSequence buffer) {

    int length = buffer.length();
    byte[] data = new byte[length / 2];

    for (int i = 0, j = 0; i < length; i += 2, j += 1) {
      data[j] = (byte) (((Character.digit(buffer.charAt(i), 16) << 4) + Character.digit(buffer.charAt(i + 1), 16)));
    }

    return data;
  }

  public static String encodeHex(byte[] value) {
    StringBuilder out = new StringBuilder();
    encodeHex(value, out);
    return out.toString();
  }

  public static void encodeHex(byte[] value, StringBuilder buffer) {

    buffer.ensureCapacity(buffer.capacity() + value.length * 2);
    for (byte b : value) {
      buffer.append(HEX_CHARS[(b >>> 4) & 0x0F]).append(HEX_CHARS[b & 0x0F]);
    }

  }

}
