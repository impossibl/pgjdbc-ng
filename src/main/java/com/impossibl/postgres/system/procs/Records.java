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

import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

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
    public Object decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {

      CompositeType compType = (CompositeType) type;

      Record record = null;

      int length = buffer.readInt();

      if (length != -1) {

        long readStart = buffer.readerIndex();

        int itemCount = buffer.readInt();

        Object[] attributeVals = new Object[itemCount];

        for (int c = 0; c < itemCount; ++c) {

          Attribute attribute = compType.getAttribute(c + 1);

          Type attributeType = context.getRegistry().loadType(buffer.readInt());

          if (attributeType.getId() != attribute.type.getId()) {

            context.refreshType(attributeType.getId());
          }

          Object attributeVal = attributeType.getBinaryCodec().decoder.decode(attributeType, null, null, buffer, context);

          attributeVals[c] = attributeVal;
        }

        if (length != buffer.readerIndex() - readStart) {
          throw new IllegalStateException();
        }

        record = new Record(compType, attributeVals);
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
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

      buffer.writeInt(-1);

      if (val != null) {

        int writeStart = buffer.writerIndex();

        Record record = (Record) val;

        Object[] attributeVals = record.getValues();

        CompositeType compType = (CompositeType) type;

        Collection<Attribute> attributes = compType.getAttributes();

        buffer.writeInt(attributes.size());

        for (Attribute attribute : attributes) {

          Type attributeType = attribute.type;

          buffer.writeInt(attributeType.getId());

          Object attributeVal = attributeVals[attribute.number - 1];

          attributeType.getBinaryCodec().encoder.encode(attributeType, buffer, attributeVal, context);
        }

        //Set length
        buffer.setInt(writeStart - 4, buffer.writerIndex() - writeStart);
      }

    }

    @Override
    public int length(Type type, Object val, Context context) throws IOException {

      int length = 4;

      if (val != null) {

        Record record = (Record) val;

        Object[] attributeVals = record.getValues();

        CompositeType compType = (CompositeType) type;

        Collection<Attribute> attributes = compType.getAttributes();

        length += 4;

        for (Attribute attribute : attributes) {

          Type attributeType = attribute.type;

          length += 4;

          int idx = attribute.number > 0 ? attribute.number - 1 : attributes.size() + attribute.number;

          Object attributeVal = attributeVals[idx];

          length += attributeType.getBinaryCodec().encoder.length(attributeType, attributeVal, context);
        }

      }

      return length;
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

        instance = readComposite(buffer, type.getDelimeter(), (CompositeType) type, context);
      }

      return new Record((CompositeType)type, instance);
    }

    Object readValue(CharSequence data, Type type, Context context) throws IOException {

      if (type instanceof CompositeType) {


        return readComposite(data, type.getDelimeter(), (CompositeType) type, context);
      }
      else {

        return type.getCodec(Format.Text).decoder.decode(type, null, null, data, context);
      }

    }

    Object[] readComposite(CharSequence data, char delim, CompositeType type, Context context) throws IOException {

      if (data.length() < 2 || (data.charAt(0) != '(' && data.charAt(data.length() - 1) != ')')) {
        return null;
      }

      data = data.subSequence(1, data.length() - 1);

      List<Object> elements = new ArrayList<>();
      StringBuilder elementTxt = new StringBuilder();
      int elementIdx = 1;

      boolean string = false;
      int opened = 0;
      int c;
      for (c = 0; c < data.length(); ++c) {

        char ch = data.charAt(c);
        switch(ch) {
          case '(':
            if (!string)
              opened++;
            else
              elementTxt.append(ch);
            break;

          case ')':
            if (!string)
              opened--;
            else
              elementTxt.append(ch);
            break;

          case '"':
            if (c < data.length() && data.charAt(c + 1) == '"') {
              elementTxt.append('"');
              c++;
            }
            else {
              string = !string;
            }
            break;

          default:

            if (ch == delim && opened == 0 && !string) {

              Object element = readValue(elementTxt.toString(), type.getAttribute(elementIdx).type, context);

              elements.add(element);

              elementTxt = new StringBuilder();
              elementIdx++;
            }
            else {

              elementTxt.append(ch);
            }

        }

      }

      Object finalElement = readValue(elementTxt.toString(), type.getAttribute(elementIdx).type, context);
      elements.add(finalElement);

      return elements.toArray();
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

      Object[] vals = val.getValues();

      for (int c = 0; c < vals.length; ++c) {

        Attribute attr = type.getAttribute(c + 1);

        Codec codec = attr.type.getCodec(Format.Text);

        StringBuilder attrOut = new StringBuilder();

        codec.encoder.encode(attr.type, attrOut, vals[c], context);

        String attrStr = attrOut.toString();

        if (needsQuotes(attrStr, delim)) {
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

    private boolean needsQuotes(String elemStr, char delim) {

      if (elemStr.isEmpty())
        return true;

      if (elemStr.equalsIgnoreCase("NULL"))
        return true;

      for (int c = 0; c < elemStr.length(); ++c) {

        char ch = elemStr.charAt(c);

        if (ch == '"' || ch == '\\' || ch == '{' || ch == '}' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
          return true;
      }

      return false;
    }

  }

}
