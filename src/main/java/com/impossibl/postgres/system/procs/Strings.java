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
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.FIELD_VARYING_LENGTH_MAX;
import static com.impossibl.postgres.types.Modifiers.LENGTH;
import static com.impossibl.postgres.types.PrimitiveType.String;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.min;

import io.netty.buffer.ByteBuf;

public class Strings extends SimpleProcProvider {

  public static final BinDecoder BINARY_DECODER = new BinDecoder();
  public static final BinEncoder BINARY_ENCODER = new BinEncoder();

  public Strings() {
    super(new TxtEncoder(), new TxtDecoder(), BINARY_ENCODER, BINARY_DECODER, new ModParser(), "text", "varchar", "bpchar", "char", "enum_", "json_", "cstring_", "unknown");
  }

  public static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return String;
    }

    @Override
    public Class<?> getOutputType() {
      return String.class;
    }

    @Override
    public String decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {

      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }

      byte[] bytes;

      Integer maxLength = (Integer) context.getSetting(FIELD_VARYING_LENGTH_MAX);
      if (maxLength != null) {
        bytes = new byte[min(maxLength, length)];
      }
      else {
        bytes = new byte[length];
      }

      buffer.readBytes(bytes);
      buffer.skipBytes(length - bytes.length);

      return new String(bytes, context.getCharset());
    }

  }

  public static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return String.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return String;
    }

    byte[] toBytes(Object val, Context context) {
      return val.toString().getBytes(context.getCharset());
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {

      if (val == null) {

        buffer.writeInt(-1);
      }
      else {

        byte[] bytes = toBytes(val, context);

        buffer.writeInt(bytes.length);

        buffer.writeBytes(bytes);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {
      return val == null ? 4 : 4 + toBytes(val, context).length;
    }

  }

  public static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return String;
    }

    @Override
    public Class<?> getOutputType() {
      return String.class;
    }

    @Override
    public String decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      return buffer.toString();
    }

  }

  public static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return String.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return String;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      if (val instanceof String) {
        buffer.append((String)val);
      }
      else if (val.getClass() == byte[].class) {
        buffer.append(new String((byte[]) val, context.getCharset()));
      }
      else {
        throw new IOException(val.getClass() + " cannot be encoded as a String");
      }
    }

  }

  static class ModParser implements Modifiers.Parser {

    @Override
    public Map<String, Object> parse(long mod) {

      Map<String, Object> mods = new HashMap<String, Object>();

      if (mod > 4) {
        mods.put(LENGTH, (int)(mod - 4));
      }

      return mods;
    }

  }

}
