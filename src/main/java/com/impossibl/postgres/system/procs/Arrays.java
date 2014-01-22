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

import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.reflect.Array.newInstance;

import io.netty.buffer.ByteBuf;

/*
 * Array codec
 *
 */
public class Arrays extends SimpleProcProvider {

  public Arrays() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "array_", "anyarray_", "oidvector", "intvector");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public Class<?> getOutputType() {
      return Object[].class;
    }

    @Override
    public Object decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {

      int length = buffer.readInt();

      int readStart = buffer.readerIndex();

      Object instance = null;

      if (length != -1) {

        ArrayType atype = ((ArrayType)type);

        //
        //Header
        //

        int dimensionCount = buffer.readInt();
        /* boolean hasNulls = */ buffer.readInt() /* == 1 ? true : false */;
        Type elementType = context.getRegistry().loadType(buffer.readInt());

        //Each Dimension
        int[] dimensions = new int[dimensionCount];
        int[] lowerBounds = new int[dimensionCount];
        for (int d = 0; d < dimensionCount; ++d) {

          //Dimension
          dimensions[d] = buffer.readInt();

          //Lower bounds
          lowerBounds[d] = buffer.readInt();
        }

        if (atype.getElementType().getId() != elementType.getId()) {
          context.refreshType(atype.getId());
        }

        //
        //Array & Elements
        //

        instance = readArray(buffer, elementType, dimensions, context);


        if (length != buffer.readerIndex() - readStart) {
          throw new IOException("invalid length");
        }

      }

      return instance;
    }

    Object readArray(ByteBuf buffer, Type type, int[] dims, Context context) throws IOException {

      if (dims.length == 0) {
        return readElements(buffer, type, 0, context);
      }
      else if (dims.length == 1) {
        return readElements(buffer, type, dims[0], context);
      }
      else {
        return readSubArray(buffer, type, dims, context);
      }

    }

    Object readSubArray(ByteBuf buffer, Type type, int[] dims, Context context) throws IOException {

      Class<?> elementClass = type.unwrap().getJavaType(Format.Binary, Collections.<String, Class<?>> emptyMap());
      Object inst = newInstance(elementClass, dims);

      int[] subDims = java.util.Arrays.copyOfRange(dims, 1, dims.length);

      for (int c = 0; c < dims[0]; ++c) {

        Array.set(inst, c, readArray(buffer, type, subDims, context));

      }

      return inst;
    }

    Object readElements(ByteBuf buffer, Type type, int len, Context context) throws IOException {

      Class<?> elementClass = type.unwrap().getJavaType(Format.Binary, Collections.<String, Class<?>> emptyMap());
      Object inst = newInstance(elementClass, len);

      for (int c = 0; c < len; ++c) {

        Array.set(inst, c, type.getBinaryCodec().decoder.decode(type, null, null, buffer, context));

      }

      return inst;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Object[].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {

      buffer.writeInt(-1);

      if (val != null) {

        int writeStart = buffer.writerIndex();

        ArrayType atype = ((ArrayType)type);
        Type elementType = atype.getElementType();

        //
        //Header
        //

        int dimensionCount = getDimensions(val.getClass(), atype.unwrapAll());
        //Dimension count
        buffer.writeInt(dimensionCount);
        //Has nulls
        buffer.writeInt(hasNulls(val) ? 1 : 0);
        //Element type
        buffer.writeInt(elementType.getId());

        //each dimension
        Object dim = val;
        for (int d = 0; d < dimensionCount; ++d) {

          int dimension = 0;
          if (dim != null)
            dimension = Array.getLength(dim);

          //Dimension
          buffer.writeInt(dimension);

          //Lower bounds
          buffer.writeInt(1);

          if (dimension == 0)
            dim = null;
          else if (dim != null)
            dim = Array.get(dim, 0);
        }

        //
        //Array & Elements

        writeArray(buffer, elementType, val, context);

        //Set length
        buffer.setInt(writeStart - 4, buffer.writerIndex() - writeStart);

      }

    }

    void writeArray(ByteBuf buffer, Type type, Object val, Context context) throws IOException {

      if (val.getClass().getComponentType().isArray() && !type.getBinaryCodec().encoder.getInputType().isArray()) {

        writeSubArray(buffer, type, val, context);
      }
      else {

        writeElements(buffer, type, val, context);
      }

    }

    void writeElements(ByteBuf buffer, Type type, Object val, Context context) throws IOException {

      int len = Array.getLength(val);

      for (int c = 0; c < len; ++c) {

        type.getBinaryCodec().encoder.encode(type, buffer, Array.get(val, c), context);
      }

    }

    void writeSubArray(ByteBuf buffer, Type type, Object val, Context context) throws IOException {

      int len = Array.getLength(val);

      for (int c = 0; c < len; ++c) {

        writeArray(buffer, type, Array.get(val, c), context);
      }

    }

    boolean hasNulls(Object value) {

      for (int c = 0, sz = Array.getLength(value); c < sz; ++c) {
        if (Array.get(value, c) == null)
          return true;
      }

      return false;
    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {

      int length = 4;

      if (val != null) {

        ArrayType arrayType = (ArrayType) type;
        Type elementType = arrayType.unwrapAll();

        int dimensionCount = getDimensions(val.getClass(), arrayType.unwrapAll());

        length += 12 + (dimensionCount * 8);

        length += subLength(elementType, dimensionCount, val, context);

      }

      return length;
    }

    private int subLength(Type type, int dimensionCount, Object val, Context context) throws IOException {

      int length = 0;

      if (dimensionCount > 1) {

        for (int d = 0, len = Array.getLength(val); d < len; ++d) {
          length += subLength(type, dimensionCount - 1, Array.get(val, d), context);
        }

      }
      else {

        for (int c = 0, len = Array.getLength(val); c < len; ++c) {
          length += type.getBinaryCodec().encoder.length(type, Array.get(val, c), context);
        }

      }

      return length;
    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public Class<?> getOutputType() {
      return Object[].class;
    }

    @Override
    public Object decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      int length = buffer.length();

      Object instance = null;

      if (length != 0) {

        ArrayType atype = ((ArrayType)type);

        instance = readArray(buffer, atype.getDelimeter(), type.unwrap(), context);
      }

      return instance;
    }

    Object readArray(CharSequence data, char delim, Type type, Context context) throws IOException {

      if (data.length() < 2 || (data.charAt(0) != '{' && data.charAt(data.length() - 1) != '}')) {
        return type.getCodec(Format.Text).decoder.decode(type, null, null, data, context);
      }

      data = data.subSequence(1, data.length() - 1);

      List<Object> elements = new ArrayList<>();
      StringBuilder elementTxt = new StringBuilder();

      boolean string = false;
      int opened = 0;
      int c;
      for (c = 0; c < data.length(); ++c) {

        char ch = data.charAt(c);
        switch(ch) {
          case '{':
            if (!string)
              opened++;
            else
              elementTxt.append(ch);
            break;

          case '}':
            if (!string)
              opened--;
            else
              elementTxt.append(ch);
            break;

          case '"':
            if (string && c < data.length() - 1 && data.charAt(c + 1) == '"') {
              elementTxt.append('"');
              c++;
            }
            else {
              string = !string;
            }
            break;

          case '\\':
            if (string) {
              ++c;
              if (c < data.length())
                elementTxt.append(data.charAt(c));
            }
            break;

          default:

            if (ch == delim && opened == 0 && !string) {

              Object element = readArray(elementTxt.toString(), delim, type, context);

              elements.add(element);

              elementTxt = new StringBuilder();
            }
            else {

              elementTxt.append(ch);
            }

        }

      }

      Object finalElement = readArray(elementTxt.toString(), delim, type, context);
      elements.add(finalElement);

      return elements.toArray();
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Object[].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      ArrayType arrayType = (ArrayType) type;

      Type elementType = arrayType.getElementType();

      writeArray(buffer, elementType.getDelimeter(), elementType, val, context);

    }

    void writeArray(StringBuilder out, char delim, Type type, Object val, Context context) throws IOException {

      Codec.Encoder encoder = type.getCodec(Format.Text).encoder;

      out.append('{');

      int len = Array.getLength(val);
      for (int c = 0; c < len; ++c) {

        Object elemVal = Array.get(val, c);
        StringBuilder elemOut = new StringBuilder();

        encoder.encode(type, elemOut, elemVal, context);

        String elemStr = elemOut.toString();

        if (needsQuotes(elemStr, delim)) {
          elemStr = elemStr.replace("\\", "\\\\");
          elemStr = elemStr.replace("\"", "\\\"");
          out.append('\"').append(elemStr).append('\"');
        }
        else {
          out.append(elemStr);
        }

        if (c < len - 1)
          out.append(delim);

      }


      out.append('}');

    }

    private boolean needsQuotes(String elemStr, char delim) {

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

  public static int getDimensions(Class<?> type, Type elementType) {
    if (type.isArray() && type != elementType.getBinaryCodec().encoder.getInputType())
      return 1 + getDimensions(type.getComponentType(), elementType);
    return 0;
  }

}
