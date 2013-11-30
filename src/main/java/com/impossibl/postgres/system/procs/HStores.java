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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

public class HStores extends SimpleProcProvider {

  public HStores() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "hstore_");
  }

  // TODO Map implementation to be used should be configurable
  private static Map<String, String> newMap(int size) {
    return new HashMap<>(Math.max(2, size));
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return Map.class;
    }

    @Override
    public Map<String, String> decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {
      // length
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      int numElements = buffer.readInt();
      Map<String, String> m = newMap(numElements);
      for (int i = 0; i < numElements; ++i) {
        String key = Strings.BINARY_DECODER.decode(type, null, null, buffer, context);
        String val = Strings.BINARY_DECODER.decode(type, null, null, buffer, context);
        m.put(key, val);
      }
      return m;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Map.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
      // length
      buffer.writeInt(-1);
      if (val != null) {
        int start = buffer.writerIndex();
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) val;
        // nb elements
        buffer.writeInt(map.size());
        for (Map.Entry<String, String> e : map.entrySet()) {
          Strings.BINARY_ENCODER.encode(type, buffer, e.getKey(), context);
          Strings.BINARY_ENCODER.encode(type, buffer, e.getValue(), context);
        }
        // Set length
        buffer.setInt(start - 4, buffer.writerIndex() - start);
      }
    }
  }

  // https://github.com/pgjdbc/pgjdbc/blob/master/org/postgresql/util/HStoreConverter.java
  static class TxtDecoder extends TextDecoder {
    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return Map.class;
    }

    @Override
    public Map<String, String> decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
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

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Map.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, String> map = (Map<String, String>) val;
      if (map.isEmpty()) {
        return;
      }
      for (Iterator<Map.Entry<String, String>> i = map.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, String> e = i.next();
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
