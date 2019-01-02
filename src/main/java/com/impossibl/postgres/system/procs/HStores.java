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
import com.impossibl.postgres.utils.TypeLiteral;

import static com.impossibl.postgres.utils.ByteBufs.lengthDecodeBinary;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncodeBinary;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class HStores extends SimpleProcProvider {

  public HStores() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "hstore_");
  }

  private static Map<String, String> newMap(int size) {
    return new HashMap<>(Math.max(2, size));
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<Map<String, String>> {

    BinDecoder() {
      super(Map::toString);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.HStore;
    }

    @Override
    public Class<Map<String, String>> getDefaultClass() {
      return new TypeLiteral<Map<String, String>>() { }.getRawType();
    }

    @Override
    protected Map<String, String> decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Type textType = context.getRegistry().loadBaseType("text");

      int numElements = buffer.readInt();
      Map<String, String> m = newMap(numElements);
      for (int i = 0; i < numElements; ++i) {
        String key = (String) lengthDecodeBinary(Strings.BINARY_DECODER, context, textType, textType.getLength(), null, buffer, String.class, null);
        String val = (String) lengthDecodeBinary(Strings.BINARY_DECODER, context, textType, textType.getLength(), null, buffer, String.class, null);
        m.put(key, val);
      }

      return m;
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<Map<String, String>> {

    BinEncoder() {
      super(TxtDecoder::parse);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.HStore;
    }

    @Override
    public Class<Map<String, String>> getDefaultClass() {
      return new TypeLiteral<Map<String, String>>() { }.getRawType();
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Map<String, String> value, Object sourceContext, ByteBuf buffer) throws IOException {

      Type textType = context.getRegistry().loadBaseType("text");

      // nb elements
      buffer.writeInt(value.size());

      for (Map.Entry<String, String> e : value.entrySet()) {
        lengthEncodeBinary(Strings.BINARY_ENCODER, context, textType, e.getKey(), null, buffer);
        lengthEncodeBinary(Strings.BINARY_ENCODER, context, textType, e.getValue(), null, buffer);
      }

    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<Map<String, String>> {

    TxtDecoder() {
      super(Map::toString);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.HStore;
    }

    @Override
    public Class<Map<String, String>> getDefaultClass() {
      return new TypeLiteral<Map<String, String>>() { }.getRawType();
    }

    @Override
    protected Map<String, String> decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      return parse(buffer);
    }

    private static Map<String, String> parse(CharSequence buffer) {

      Map<String, String> m = newMap(10);

      String s = buffer.toString();
      int pos = 0;
      StringBuilder sb = new StringBuilder();

      while (pos < buffer.length()) {

        sb.setLength(0);
        int start = s.indexOf('"', pos);
        int end = appendUntilQuote(sb, s, start);
        String key = sb.toString();
        pos = end + 3;

        String val;
        if (s.charAt(pos) == 'N') {
          val = null;
          pos += 4;
        }
        else {
          sb.setLength(0);
          end = appendUntilQuote(sb, s, pos);
          val = sb.toString();
          pos = end;
        }

        pos++;

        m.put(key, val);
      }

      return m;
    }

    private static int appendUntilQuote(StringBuilder sb, String s, int pos) {
      for (pos += 1; pos < s.length(); pos++) {
        char ch = s.charAt(pos);
        if (ch == '"') {
          break;
        }
        if (ch == '\\') {
          pos++;
          ch = s.charAt(pos);
        }
        sb.append(ch);
      }
      return pos;
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<Map<String, String>> {

    protected TxtEncoder() {
      super(TxtDecoder::parse);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.HStore;
    }

    @Override
    public Class<Map<String, String>> getDefaultClass() {
      return new TypeLiteral<Map<String, String>>() { }.getRawType();
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Map<String, String> value, Object sourceContext, StringBuilder buffer) throws IOException {

      if (value.isEmpty()) {
        return;
      }

      for (Map.Entry<String, String> e : value.entrySet()) {

        appendEscaped(buffer, e.getKey());

        buffer.append("=>");

        appendEscaped(buffer, e.getValue());

        buffer.append(", ");
      }

      buffer.setLength(buffer.length() - 2);
    }

    private static void appendEscaped(StringBuilder sb, Object val) {
      if (val != null) {
        sb.append('"');
        String s = val.toString();
        for (int pos = 0; pos < s.length(); pos++) {
          char ch = s.charAt(pos);
          if (ch == '"' || ch == '\\') {
            sb.append('\\');
          }
          sb.append(ch);
        }
        sb.append('"');
      }
      else {
        sb.append("NULL");
      }
    }

  }

}
