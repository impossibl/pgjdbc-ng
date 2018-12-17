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

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;


public class FieldBuffersRowData extends AbstractReferenceCounted implements UpdatableRowData, ReferenceCounted {

  private ByteBuf[] fieldBuffers;
  private ByteBufAllocator alloc;

  public FieldBuffersRowData(ResultField[] fields, ByteBufAllocator alloc) {
    this(new ByteBuf[fields.length], alloc);
  }

  public FieldBuffersRowData(ByteBuf[] fieldBuffers, ByteBufAllocator alloc) {
    this.fieldBuffers = fieldBuffers;
    this.alloc = alloc;
  }

  @Override
  public int getFieldCount() {
    return fieldBuffers.length;
  }

  @Override
  public Object getField(int fieldIdx, ResultField field, Context context, Class<?> targetClass, Object targetContext) throws IOException {

    Type type = field.getTypeRef().getType();

    ByteBuf fieldBuffer = fieldBuffers[fieldIdx];
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
  public void updateField(int columnIndex, ResultField field, Context context, Object source, Object sourceContext) throws IOException {

    ByteBuf byteBuf = fieldBuffers[columnIndex];
    if (byteBuf == null) {
      if (source == null) return;
      byteBuf = alloc.buffer();
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

    Type type = field.getTypeRef().getType();

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
  public ByteBuf[] getFieldBuffers() {
    return fieldBuffers;
  }

  @Override
  protected void deallocate() {
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.release(fieldBuffer);
    }
  }

  @Override
  public FieldBuffersRowData touch(Object hint) {
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.touch(fieldBuffer, hint);
    }
    return this;
  }

  @Override
  public UpdatableRowData duplicateForUpdate() {
    ByteBuf[] fieldBuffers = this.fieldBuffers.clone();
    for (ByteBuf fieldBuffer : fieldBuffers) {
      ReferenceCountUtil.retain(fieldBuffer);
    }
    return new FieldBuffersRowData(fieldBuffers, alloc);
  }

}
