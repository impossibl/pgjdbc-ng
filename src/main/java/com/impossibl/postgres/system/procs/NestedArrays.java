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

import com.impossibl.postgres.jdbc.PGBuffersArray;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.NestedArrayType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.CompositeCharSequence;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;


/*
 * Array codec
 *
 */
public class NestedArrays {

  public static final BinDecoder BINARY_DECODER = new BinDecoder();
  public static final TxtDecoder TEXT_DECODER = new TxtDecoder();

  static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Array.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      NestedArrayType atype = (NestedArrayType) type;
      CompositeByteBuf compBuf = (CompositeByteBuf)buffer;

      ByteBuf[] elementBufs = new ByteBuf[compBuf.numComponents()];
      for (int c = 0; c < elementBufs.length; ++c) {
        elementBufs[c] = compBuf.component(c).retain();
      }

      return new PGBuffersArray(context, atype, atype.getElementFormat(), elementBufs, atype.getDimensions());
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Array;
    }

    @Override
    public Class<?> getDefaultClass() {
      return java.sql.Array.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      ByteBufAllocator byteBufAllocator = context.getProtocol().getChannel().alloc();

      NestedArrayType atype = (NestedArrayType) type;
      CompositeCharSequence compBuf = (CompositeCharSequence)buffer;
      CharSequence[] elementBufs = compBuf.getComponents();

      ByteBuf[] elementBinaryBufs = new ByteBuf[elementBufs.length];
      for (int elementIdx = 0; elementIdx < elementBufs.length; ++elementIdx) {
        elementBinaryBufs[elementIdx] = ByteBufUtil.writeUtf8(byteBufAllocator, elementBufs[elementIdx]);
      }

      return new PGBuffersArray(context, atype, FieldFormat.Text, elementBinaryBufs, atype.getDimensions());
    }

  }

}
