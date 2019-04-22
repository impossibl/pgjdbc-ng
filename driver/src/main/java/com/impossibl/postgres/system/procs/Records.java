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

import com.impossibl.postgres.jdbc.PGBuffersStruct;
import com.impossibl.postgres.jdbc.PGSQLInput;
import com.impossibl.postgres.jdbc.PGSQLOutput;
import com.impossibl.postgres.jdbc.PGStruct;
import com.impossibl.postgres.jdbc.PGValuesStruct;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.CustomTypes.lookupCustomType;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncodeBinary;

import java.io.IOException;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Struct;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Character.isWhitespace;
import static java.util.Arrays.fill;

import io.netty.buffer.ByteBuf;


public class Records extends SimpleProcProvider {

  public Records() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "record_");
  }

  static PGStruct convertInput(Context context, Type type, Object value) throws IOException {

    PGStruct struct;
    if (value instanceof PGStruct) {
      struct = (PGStruct) value;
    }
    else if (value instanceof SQLData) {
      PGSQLOutput out = new PGSQLOutput(context);
      SQLData data = (SQLData) value;
      try {

        data.writeSQL(out);

        struct = new PGValuesStruct(context, data.getSQLTypeName(), out.getAttributeTypes(), out.getAttributeValues());
      }
      catch (SQLException e) {
        throw new IOException(e);
      }
    }
    else {
      throw new ConversionException(value.getClass(), type);
    }

    return struct;
  }

  static <Buffer> Object convertOutput(Context context, Type type, Type[] attributeTypes, Buffer[] attributeBuffers, Class<?> targetClass, InputFactory<Buffer> inputFactory, StructFactory<Buffer> structFactory) throws IOException {

    if (Struct.class.isAssignableFrom(targetClass)) {
      targetClass = lookupCustomType(type, context.getCustomTypeMap(), targetClass);
    }

    Object result;

    if (SQLData.class.isAssignableFrom(targetClass)) {
      SQLData data;
      try {
        data = (SQLData) targetClass.getConstructor().newInstance();
      }
      catch (Exception e) {
        throw new IOException("Unable to instantiate custom type; an accessible no-arg constructor is required", e);
      }

      try {
        PGSQLInput<Buffer> input = inputFactory.create(context, attributeTypes, attributeBuffers);
        data.readSQL(input, type.getQualifiedName().toString());
      }
      catch (SQLException e) {
        throw new IOException(e);
      }

      result = data;
    }
    else if (Struct.class.isAssignableFrom(targetClass)) {
      result = structFactory.create(context, type.getQualifiedName().toString(), attributeTypes, attributeBuffers);
    }
    else {
      throw new ConversionException(type, targetClass);
    }

    return result;
  }

  static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return PGStruct.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Registry registry = context.getRegistry();

      int length = buffer.readableBytes();
      long readStart = buffer.readerIndex();

      int itemCount = buffer.readInt();

      Type[] attributeTypes = new Type[itemCount];
      ByteBuf[] attributeBuffers = new ByteBuf[itemCount];

      for (int c = 0; c < itemCount; ++c) {

        Type attributeType = registry.loadType(buffer.readInt());
        attributeTypes[c] = attributeType;

        int attributeLen = buffer.readInt();
        if (attributeLen != -1) {
          ByteBuf attributeBuffer = PGBuffersStruct.Binary.ALLOC.buffer(attributeLen);
          buffer.readBytes(attributeBuffer, attributeLen);
          attributeBuffers[c] = attributeBuffer;
        }
      }

      if (length != buffer.readerIndex() - readStart) {
        throw new IllegalStateException();
      }

      return convertOutput(context, type, attributeTypes, attributeBuffers, targetClass, PGSQLInput.Binary::new, PGBuffersStruct.Binary::new);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      PGStruct struct = convertInput(context, type, value);
      Type[] attributeTypes = struct.getAttributeTypes();
      Object[] attributeValues = struct.getAttributes(context);

      buffer.writeInt(attributeValues.length);

      for (int c = 0; c < attributeValues.length; ++c) {

        Type attributeType = attributeTypes[c];
        Object attributeValue = attributeValues[c];

        buffer.writeInt(attributeType.getId());

        lengthEncodeBinary(attributeType.getBinaryCodec().getEncoder(), context, attributeType, attributeValue, null, buffer);
      }
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return PGStruct.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      List<CharSequence> attributeBuffers = new ArrayList<>();
      parseAttributeBuffers(type.getDelimeter(), buffer, attributeBuffers);

      Type[] attributeTypes = new Type[attributeBuffers.size()];
      fill(attributeTypes, context.getRegistry().loadBaseType("text"));

      return convertOutput(context, type, attributeTypes, attributeBuffers.toArray(new CharSequence[0]), targetClass, PGSQLInput.Text::new, PGBuffersStruct.Text::new);
    }

    void parseAttributeBuffers(char delim, CharSequence buffer, List<CharSequence> attributes) {

      int len = buffer.length();
      StringBuilder attributeText = null;

      int charIdx;
      int lastDelimIdx = -1;

      scan:
      for (charIdx = 0; charIdx < len; ++charIdx) {

        char ch = buffer.charAt(charIdx);
        switch (ch) {

          case '(':
            lastDelimIdx = charIdx;
            break;

          case ')':
            addTextElement(attributeText, lastDelimIdx == charIdx - 1, attributes);
            break scan;

          case '"':
            attributeText = new StringBuilder();
            charIdx = readString(buffer, charIdx, attributeText);
            break;

          default:

            // Eat whitespace
            if (isWhitespace(ch)) {
              charIdx = skipWhitespace(buffer, charIdx);
              break;
            }

            if (ch == delim) {
              attributeText = addTextElement(attributeText, lastDelimIdx == charIdx - 1, attributes);
              lastDelimIdx = charIdx;
              break;
            }

            if (attributeText == null) {
              attributeText = new StringBuilder();
            }
            attributeText.append(ch);
        }

      }

    }

    StringBuilder addTextElement(StringBuilder text, boolean empty, List<CharSequence> attributes) {
      if (empty) {
        attributes.add(null);
      }
      else {
        String textStr = text.toString();
        if (textStr.equalsIgnoreCase("NULL")) {
          attributes.add(null);
        }
        else {
          attributes.add(textStr);
        }
      }
      return null;
    }

    int skipWhitespace(CharSequence data, int start) {

      int len = data.length();
      int c = start;
      while (c < len && isWhitespace(data.charAt(c))) {
        ++c;
      }
      return c;
    }

    int readString(CharSequence data, int start, StringBuilder string) {

      int len = data.length();
      int c;

      scan:
      for (c = start + 1; c < len; ++c) {

        char ch = data.charAt(c);
        switch (ch) {
          case '"':
            if (c < data.length() - 1 && data.charAt(c + 1) == '"') {
              ++c;
              string.append('"');
              break;
            }
            else {
              break scan;
            }

          case '\\':
            ++c;
            if (c < data.length()) {
              ch = data.charAt(c);
            }

          default:
            string.append(ch);
        }

      }

      return c;
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      char delim = type.getDelimeter();

      PGStruct struct = convertInput(context, type, value);
      Type[] attributeTypes = struct.getAttributeTypes();
      Object[] attributeValues = struct.getAttributes(context);

      buffer.append('(');

      for (int c = 0; c < attributeValues.length; ++c) {

        Type attributeType = attributeTypes[c];
        Object attributeValue = attributeValues[c];

        StringBuilder attributeOut = new StringBuilder();

        attributeType.getTextCodec().getEncoder()
            .encode(context, attributeType, attributeValue, null, attributeOut);

        String attributeStr = attributeOut.toString();

        if (needsQuotes(attributeStr, delim)) {
          attributeStr = attributeStr.replace("\\", "\\\\");
          attributeStr = attributeStr.replace("\"", "\\\"");
          buffer.append('\"').append(attributeStr).append('\"');
        }
        else {
          buffer.append(attributeStr);
        }

        if (c < attributeValues.length - 1) {
          buffer.append(delim);
        }
      }

      buffer.append(')');
    }

    private static boolean needsQuotes(String elemStr, char delim) {

      if (elemStr.isEmpty())
        return true;

      if (elemStr.equalsIgnoreCase("NULL"))
        return true;

      for (int c = 0; c < elemStr.length(); ++c) {

        char ch = elemStr.charAt(c);

        if (ch == delim || ch == '"' || ch == '\\' || ch == '{' || ch == '}' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
          return true;
      }

      return false;
    }

  }

}


interface InputFactory<Buffer> {
  PGSQLInput<Buffer> create(Context context, Type[] attributeTypes, Buffer[] attributeBuffers);
}

interface StructFactory<Buffer> {
  PGBuffersStruct<Buffer> create(Context context, String typeName, Type[] attributeTypes, Buffer[] attributeBuffers);
}
