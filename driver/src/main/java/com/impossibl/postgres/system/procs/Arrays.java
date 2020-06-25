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

import com.impossibl.postgres.jdbc.PGArray;
import com.impossibl.postgres.jdbc.PGBuffersArray;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.utils.ByteBufs.lengthEncodeBinary;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Character.isWhitespace;
import static java.util.Arrays.copyOf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;


/*
 * Array codec
 *
 */
public class Arrays extends SimpleProcProvider {

  public Arrays() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "array_", "anyarray_");
  }

  static Object convertOutput(PGArray array, Class<?> targetClass) throws IOException {

    if (targetClass == java.sql.Array.class) {
      return array;
    }

    if (targetClass.isArray()) {
      try {
        Object result = array.getArray(targetClass.getComponentType());
        array.free();
        return result;
      }
      catch (SQLException e) {
        throw new IOException(e);
      }
    }

    throw new ConversionException(array.getType(), targetClass);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Array.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      ArrayType atype = (ArrayType) type;

      //
      //Header
      //

      int dimensionCount = buffer.readInt();
      /* int flags = */ buffer.readInt();
      Type elementType = context.getRegistry().loadType(buffer.readInt());

      if (!atype.getElementType().equals(elementType)) {
        throw new IllegalStateException("Array element type mismatch");
      }

      //Each Dimension
      int[] dimensions = new int[dimensionCount];
      // int[] lowerBounds = new int[dimensionCount];
      for (int d = 0; d < dimensionCount; ++d) {

        //Dimension
        dimensions[d] = buffer.readInt();

        //Lower bounds
        /* lowerBounds[d] = */ buffer.readInt();
      }

      //
      //Array & Elements
      //

      int totalItems = strideOfDimensions(dimensions);

      ByteBuf[] elementBufs = new ByteBuf[totalItems];
      for (int elementIdx = 0; elementIdx < totalItems; ++elementIdx) {
        int elementLength = buffer.readInt();
        if (elementLength != -1) {
          elementBufs[elementIdx] = buffer.readRetainedSlice(elementLength);
        }
        else {
          elementBufs[elementIdx] = null;
        }
      }

      return convertOutput(new PGBuffersArray(context, atype, FieldFormat.Binary, elementBufs, dimensions), targetClass);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Class<?> valueType = value.getClass();

      if (value instanceof PGArray) {
        try {
          value = ((PGArray) value).getArray();
        }
        catch (SQLException e) {
          throw new IOException(e);
        }
      }
      else if (!valueType.isArray()) {
        throw new ConversionException(valueType, type);
      }

      ArrayType atype = (ArrayType) type;
      Type elementType = atype.getElementType();

      //
      //Header
      //

      int dimensionCount = getDimensions(value.getClass());
      //Dimension count
      buffer.writeInt(dimensionCount);
      //Has nulls
      buffer.writeInt(hasNulls(value) ? 1 : 0);
      //Element type
      buffer.writeInt(elementType.getId());

      //each dimension
      Object array = value;
      for (int d = 0; d < dimensionCount; ++d) {

        int dimension = array != null ? Array.getLength(array) : 0;

        //Dimension
        buffer.writeInt(dimension);

        //Lower bounds
        buffer.writeInt(1);

        if (dimension == 0) {
          array = null;
        }
        else {
          array = Array.get(array, 0);
        }
      }

      //
      //Array & Elements

      writeArray(context, elementType, value, buffer);

    }

    void writeArray(Context context, Type type, Object val, ByteBuf buffer) throws IOException {

      if (val.getClass().getComponentType().isArray()) {

        writeSubArray(context, type, val, buffer);
      }
      else {

        writeElements(context, type, val, buffer);
      }

    }

    void writeElements(Context context, Type type, Object val, ByteBuf buffer) throws IOException {

      int len = Array.getLength(val);

      for (int c = 0; c < len; ++c) {

        Object element = Array.get(val, c);

        lengthEncodeBinary(type.getBinaryCodec().getEncoder(), context, type, element, null, buffer);
      }

    }

    void writeSubArray(Context context, Type type, Object val, ByteBuf buffer) throws IOException {

      int len = Array.getLength(val);

      for (int c = 0; c < len; ++c) {

        writeArray(context, type, Array.get(val, c), buffer);
      }

    }

    boolean hasNulls(Object value) {

      for (int c = 0, sz = Array.getLength(value); c < sz; ++c) {
        if (Array.get(value, c) == null)
          return true;
      }

      return false;
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Array.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      ByteBufAllocator byteBufAllocator = context.getAllocator();
      ArrayType atype = (ArrayType) type;

      List<CharSequence> elementBuffers = new ArrayList<>();

      int[] dimensions = parseElementBuffers(atype.getDelimeter(), buffer, elementBuffers);

      ByteBuf[] elementBinaryBuffers = new ByteBuf[elementBuffers.size()];
      for (int elementIdx = 0; elementIdx < elementBuffers.size(); ++elementIdx) {
        CharSequence elementBuffer = elementBuffers.get(elementIdx);
        if (elementBuffer != null) {
          elementBinaryBuffers[elementIdx] = ByteBufUtil.writeUtf8(byteBufAllocator, elementBuffer);
        }
      }

      return convertOutput(new PGBuffersArray(context, atype, FieldFormat.Text, elementBinaryBuffers, dimensions), targetClass);
    }

    int[] parseElementBuffers(char delim, CharSequence buffer, List<CharSequence> elements) throws IOException {

      int[] dimensions = new int[0];
      int len = buffer.length();
      StringBuilder elementText = null;
      boolean quoted = false;

      int depth = -1;
      int charIdx;

      scan:
      for (charIdx = 0; charIdx < len; ++charIdx) {

        char ch = buffer.charAt(charIdx);
        switch (ch) {

          case '{':
            ++depth;
            if (dimensions.length < depth + 1) {
              dimensions = copyOf(dimensions, depth + 1);
            }
            dimensions[depth] = 0;
            break;

          case '}':
            if (elementText != null) {
              ++dimensions[depth];
              elementText = addTextElement(elementText, quoted, elements);
              quoted = false;
            }
            if (--depth < 0) {
              break scan;
            }
            else {
              ++dimensions[depth];
            }
            break;

          case '"':
            elementText = new StringBuilder();
            charIdx = readString(buffer, charIdx, elementText);
            quoted = true;
            break;

          case '[':
            if (depth == -1) {
              while (ch != '=' && charIdx < len) ch = buffer.charAt(++charIdx);
              break;
            }

          default:

            // Eat whitespace
            if (isWhitespace(ch)) {
              charIdx = skipWhitespace(buffer, charIdx);
              break;
            }

            if (ch == delim) {
              if (elementText != null) {
                ++dimensions[depth];
                elementText = addTextElement(elementText, quoted, elements);
                quoted = false;
              }
              break;
            }

            if (elementText == null) {
              elementText = new StringBuilder();
            }
            elementText.append(ch);
        }

      }

      return dimensions;
    }

    StringBuilder addTextElement(StringBuilder text, boolean quoted, List<CharSequence> elements) {
      String textStr = text.toString();
      if (!quoted && textStr.equalsIgnoreCase("NULL")) {
        elements.add(null);
      }
      else {
        elements.add(textStr);
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

    int readString(CharSequence data, int start, Appendable out) throws IOException {

      int len = data.length();
      int charIdx;

      scan:
      for (charIdx = start + 1; charIdx < len; ++charIdx) {
        char ch = data.charAt(charIdx);
        switch (ch) {
          case '"':
            if (charIdx < data.length() - 1 && data.charAt(charIdx + 1) == '"') {
              ++charIdx;
              out.append('"');
              break;
            }
            else {
              break scan;
            }

          case '\\':
            ++charIdx;
            if (charIdx < data.length()) {
              ch = data.charAt(charIdx);
            }

          default:
            out.append(ch);
        }
      }

      return charIdx;
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Class<?> valueType = value.getClass();

      if (value instanceof PGArray) {
        try {
          value = ((PGArray) value).getArray();
        }
        catch (SQLException e) {
          throw new IOException(e);
        }
      }
      else if (!valueType.isArray()) {
        throw new ConversionException(valueType, type);
      }

      ArrayType arrayType = (ArrayType) type;

      Type elementType = arrayType.getElementType();

      writeArray(context, elementType, elementType.getDelimeter(), value, sourceContext, buffer);

    }

    void writeArray(Context context, Type elementType, char delim, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Type.Codec.Encoder<StringBuilder> encoder = elementType.getTextCodec().getEncoder();

      buffer.append('{');

      int len = Array.getLength(value);
      for (int c = 0; c < len; ++c) {

        Object elementValue = Array.get(value, c);
        StringBuilder elementBuffer = new StringBuilder();

        if (elementValue == null) {
          buffer.append("NULL");
        }
        else if (elementValue.getClass().isArray() && elementValue.getClass() != byte[].class) {
          writeArray(context, elementType, delim, elementValue, sourceContext, buffer);
        }
        else {
          encoder.encode(context, elementType, elementValue, sourceContext, elementBuffer);

          String elemStr = elementBuffer.toString();

          if (needsQuotes(elemStr, delim)) {
            elemStr = elemStr.replace("\\", "\\\\");
            elemStr = elemStr.replace("\"", "\\\"");
            buffer.append('\"').append(elemStr).append('\"');
          }
          else {
            buffer.append(elemStr);
          }

        }

        if (c < len - 1)
          buffer.append(delim);

      }

      buffer.append('}');
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

  private static int getDimensions(Class<?> type) {
    if (type.isArray())
      return 1 + getDimensions(type.getComponentType());
    return 0;
  }

  public static int strideOfDimensions(int[] dimensions) {
    return strideOfDimensions(dimensions, 0);
  }

  public static int strideOfDimensions(int[] dimensions, int offset) {
    if (dimensions.length == 0) return 0;
    int stride = 1;
    for (int c = offset; c < dimensions.length; ++c) {
      stride *= dimensions[c];
    }
    return stride;
  }

}
