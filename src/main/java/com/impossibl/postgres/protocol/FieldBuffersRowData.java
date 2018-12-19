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
package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.Preconditions;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;


public class FieldBuffersRowData implements UpdatableRowData {

  private ResultField[] fields;
  private ByteBuf[] fieldBuffers;

  public FieldBuffersRowData(ResultField[] fields) {
    this(fields, new ByteBuf[fields.length]);
  }

  public FieldBuffersRowData(ResultField[] fields, ByteBuf[] fieldBuffers) {
    Preconditions.checkArgument(fieldBuffers.length == fields.length);
    this.fields = fields;
    this.fieldBuffers = fieldBuffers;
  }

  @Override
  public ResultField[] getColumnFields() {
    return fields;
  }

  @Override
  public int getColumnCount() {
    return fields.length;
  }

  @Override
  public Object getColumn(int columnIndex, Context context, Class<?> targetClass, Object targetContext) throws IOException {

    ResultField field = fields[columnIndex];
    Type type = field.getTypeRef().get();

    ByteBuf fieldBuffer = fieldBuffers[columnIndex];
    fieldBuffer.resetReaderIndex();

    Object result;

    switch (field.getFormat()) {
      case Text: {
        String fieldStr = fieldBuffer.toString(context.getCharset());
        Type.Codec.Decoder<CharSequence> decoder = type.getTextCodec().getDecoder();
        result = decoder.decode(context, type, field.getTypeLength(), field.getTypeModifier(), fieldStr, targetClass, targetContext);
      }
      break;

      case Binary: {
        Type.Codec.Decoder<ByteBuf> decoder = type.getBinaryCodec().getDecoder();
        result = decoder.decode(context, type, field.getTypeLength(), field.getTypeModifier(), fieldBuffer, targetClass, targetContext);
      }
      break;

      default:
        throw new IllegalStateException();
    }

    return result;
  }

  @Override
  public void updateColumn(int columnIndex, Context context, Object source, Object sourceContext) throws IOException {

    ByteBuf byteBuf = fieldBuffers[columnIndex];
    if (byteBuf == null) {
      if (source == null) return;
      byteBuf = context.getProtocol().getChannel().alloc().buffer();
      fieldBuffers[columnIndex] = byteBuf;
    }
    else {
      byteBuf.resetReaderIndex().resetWriterIndex();
    }

    if (source == null) {
      ReferenceCountUtil.release(byteBuf);
      fieldBuffers[columnIndex] = null;
      return;
    }

    ResultField field = fields[columnIndex];
    Type type = field.getTypeRef().get();

    switch (field.getFormat()) {
      case Text: {
        StringBuilder out = new StringBuilder();
        type.getTextCodec().getEncoder()
            .encode(context, type, source, sourceContext, out);
        ByteBufUtil.writeUtf8(byteBuf, out);
      }
      break;

      case Binary: {
        type.getBinaryCodec().getEncoder()
            .encode(context, type, source, sourceContext, byteBuf);
      }
      break;
    }

  }

  @Override
  public ByteBuf[] getColumnBuffers() {
    return fieldBuffers;
  }

  @Override
  public FieldBuffersRowData retain() {
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.retain(fieldBuffer);
    }
    return this;
  }

  @Override
  public void release() {
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.release(fieldBuffer);
    }
  }

  @Override
  public void touch(Object hint) {
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.touch(fieldBuffer, hint);
    }
  }

  @Override
  public UpdatableRowData duplicateForUpdate() {
    ByteBuf[] fieldBuffers = this.fieldBuffers.clone();
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.retain(fieldBuffer);
    }
    return new FieldBuffersRowData(fields, fieldBuffers);
  }

}
