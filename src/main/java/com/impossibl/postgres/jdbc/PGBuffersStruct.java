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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;


public abstract class PGBuffersStruct<Buffer> extends PGStruct {

  public static class Binary extends PGBuffersStruct<ByteBuf> {

    public static final ByteBufAllocator ALLOC = new UnpooledByteBufAllocator(false, true);

    public static Binary encode(Context context, CompositeType type, Object[] values) throws SQLException, IOException {

      Type[] attributeTypes = new Type[values.length];
      ByteBuf[] attributeBuffers = new ByteBuf[values.length];

      for (int attributeIdx = 0; attributeIdx < attributeBuffers.length; ++attributeIdx) {

        ByteBuf attributeBuffer = ALLOC.buffer();
        Object value = values[attributeIdx];
        if (value == null) {
          attributeTypes[attributeIdx] = context.getRegistry().loadBaseType("text");
          attributeBuffers[attributeIdx] = null;
          continue;
        }

        Type attributeType = JDBCTypeMapping.getType(JDBCTypeMapping.getSQLTypeCode(value.getClass()), value, context.getRegistry());
        if (attributeType == null) {
          throw new IOException("Unable to determine type of attribute " + (attributeIdx + 1));
        }

        attributeType.getBinaryCodec().getEncoder()
            .encode(context, type, values[attributeIdx], null, attributeBuffer);

        attributeTypes[attributeIdx] = attributeType;
        attributeBuffers[attributeIdx] = attributeBuffer;
      }

      return new Binary(context, type.getQualifiedName().toString(), attributeTypes, attributeBuffers);
    }

    public Binary(Context context, String typeName, Type[] attributeTypes, ByteBuf[] attributeBuffers) {
      super(context, typeName, attributeTypes, attributeBuffers);
    }

    @Override
    protected Object getAttribute(Context context, Type type, ByteBuf buffer) throws IOException {
      return type.getBinaryCodec().getDecoder().decode(context, type, type.getLength(), null, buffer, null, null);
    }

  }

  public static class Text extends PGBuffersStruct<CharSequence> {

    public Text(Context context, String typeName, Type[] attributeTypes, CharSequence[] attributeBuffers) {
      super(context, typeName, attributeTypes, attributeBuffers);
    }

    @Override
    protected Object getAttribute(Context context, Type type, CharSequence buffer) throws IOException {
      return type.getTextCodec().getDecoder().decode(context, type, type.getLength(), null, buffer, null, null);
    }

  }


  private Buffer[] attributeBuffers;

  private PGBuffersStruct(Context context, String typeName, Type[] attributeTypes, Buffer[] attributeBuffers) {
    super(context, typeName, attributeTypes);
    this.attributeBuffers = attributeBuffers;
  }

  protected abstract Object getAttribute(Context context, Type type, Buffer buffer) throws IOException;

  @Override
  public Object[] getAttributes(Context context) throws IOException {

    Object[] attributeValues = new Object[attributeBuffers.length];

    for (int attributeIdx = 0; attributeIdx < attributeValues.length; ++attributeIdx) {
      attributeValues[attributeIdx] = getAttribute(context, attributeTypes[attributeIdx], attributeBuffers[attributeIdx]);
    }

    return attributeValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PGBuffersStruct struct = (PGBuffersStruct) o;
    return Objects.equals(context, struct.context) &&
        Objects.equals(typeName, struct.typeName) &&
        Arrays.equals(attributeTypes, struct.attributeTypes) &&
        Arrays.equals(attributeBuffers, struct.attributeBuffers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, typeName, attributeTypes, attributeBuffers);
  }

}
