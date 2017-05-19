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

import com.impossibl.postgres.api.data.Record;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.PsuedoType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.Character.isWhitespace;

import io.netty.buffer.ByteBuf;

public class Records extends SimpleProcProvider {

  public Records() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "record_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return Record;
    }

    @Override
    public Class<?> getOutputType() {
      return Record.class;
    }

    @Override
    public Object decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {

      CompositeType compType;
      if (type instanceof CompositeType) {
        compType = (CompositeType) type;
      }
      else if (type instanceof PsuedoType && type.getName().equals("record")) {
        compType = null;
      }
      else {
        throw new IOException("Unsupported type for Record decode");
      }

      List<Type> attributeTypes = new ArrayList<>();

      Record record = null;

      int length = buffer.readInt();

      if (length != -1) {

        long readStart = buffer.readerIndex();

        int itemCount = buffer.readInt();

        Object[] attributeVals = new Object[itemCount];

        for (int c = 0; c < itemCount; ++c) {


          Type attributeType = context.getRegistry().loadType(buffer.readInt());
          attributeTypes.add(attributeType);

          if (compType != null) {

            Attribute attribute = compType.getAttribute(c + 1);
            if (attributeType.getId() != attribute.getType().getId()) {

              context.refreshType(attributeType.getId());
            }

          }

          Object attributeVal = attributeType.getBinaryCodec().getDecoder().decode(attributeType, null, null, buffer, context);

          attributeVals[c] = attributeVal;
        }

        if (length != buffer.readerIndex() - readStart) {
          throw new IllegalStateException();
        }

        record = new Record(type.getName(), attributeTypes.toArray(new Type[attributeTypes.size()]), attributeVals);
      }

      return record;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Record.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return Record;
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {

      buffer.writeInt(-1);

      if (val != null) {

        int writeStart = buffer.writerIndex();

        Record record = (Record) val;

        Object[] attributeVals = record.getAttributeValues();

        CompositeType compType = (CompositeType) type;

        Collection<Attribute> attributes = compType.getAttributes();

        buffer.writeInt(attributes.size());

        for (Attribute attribute : attributes) {

          Type attributeType = attribute.getType();

          buffer.writeInt(attributeType.getId());

          Object attributeVal = attributeVals[attribute.getNumber() - 1];

          attributeType.getBinaryCodec().getEncoder().encode(attributeType, buffer, attributeVal, context);
        }

        //Set length
        buffer.setInt(writeStart - 4, buffer.writerIndex() - writeStart);
      }

    }

  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Record;
    }

    @Override
    public Class<?> getOutputType() {
      return Record.class;
    }

    @Override
    public Record decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {

      int length = buffer.length();

      Object[] instance = null;

      if (length != 0) {

        List<Object> fields = new ArrayList<>();
        readComposite(buffer, 0, type.getDelimeter(), (CompositeType) type, context, fields);
        instance = fields.toArray();
      }

      return new Record(type.getName(), ((CompositeType) type).getAttributesTypes(), instance);
    }

    int readComposite(CharSequence data, int start, char delim, CompositeType type, Context context, List<Object> fields) throws IOException {

      if (data.equals("()")) {
        return start + 1;
      }

      StringBuilder elementTxt = null;

      int c;
      int len = data.length();

    scan:
      for (c = start + 1; c < len; ++c) {

        char ch = data.charAt(c);
        switch (ch) {

          case '(':
            List<Object> subElements = new ArrayList<>();
            c = readComposite(data, c, delim, type, context, subElements);
            fields.add(subElements.toArray());
            break;

          case ')':
            if (elementTxt != null) {
              fields.add(decode(elementTxt.toString(), type.getAttribute(fields.size() + 1).getType(), context));
            }
            break scan;

          case '"':
            elementTxt = elementTxt != null ? elementTxt : new StringBuilder();
            c = readString(data, c, elementTxt);
            break;

          default:

            // Eat whitespace
            if (isWhitespace(ch)) {
              c = skipWhitespace(data, c);
              break;
            }

            if (ch == delim) {
              if (elementTxt != null) {
                fields.add(decode(elementTxt.toString(), type.getAttribute(fields.size() + 1).getType(), context));
              }
              elementTxt = null;
              break;
            }

            elementTxt = elementTxt != null ? elementTxt : new StringBuilder();
            elementTxt.append(ch);
        }

      }

      return c;
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

    Object decode(String elementTxt, Type type, Context context) throws IOException {
      if (elementTxt.equals("NULL")) {
        return null;
      }
      return type.getCodec(Format.Text).getDecoder().decode(type, null, null, elementTxt, context);
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Record.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Record;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {

      writeComposite(buffer, type.getDelimeter(), (CompositeType) type, (Record) val, context);

    }

    void writeComposite(StringBuilder out, char delim, CompositeType type, Record val, Context context) throws IOException {

      out.append('(');

      Object[] vals = val.getAttributeValues();

      for (int c = 0; c < vals.length; ++c) {

        Attribute attr = type.getAttribute(c + 1);

        Codec codec = attr.getType().getCodec(Format.Text);

        StringBuilder attrOut = new StringBuilder();

        codec.getEncoder().encode(attr.getType(), attrOut, vals[c], context);

        String attrStr = attrOut.toString();

        if (needsQuotes(attrStr, delim)) {
          attrStr = attrStr.replace("\\", "\\\\");
          attrStr = attrStr.replace("\"", "\\\"");
          out.append('\"').append(attrStr).append('\"');
        }
        else {
          out.append(attrStr);
        }

        if (c < vals.length - 1)
          out.append(delim);
      }

      out.append(')');

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
