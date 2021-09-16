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

import com.impossibl.postgres.protocol.FieldBuffersRowData;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.NestedArrayType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.ArrayUtils.getDimensions;
import static com.impossibl.postgres.jdbc.ArrayUtils.getElementType;
import static com.impossibl.postgres.system.CustomTypes.lookupCustomType;
import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.procs.Arrays.strideOfDimensions;
import static com.impossibl.postgres.utils.Types.boxType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;


public class PGBuffersArray extends PGArray {

  private FieldFormat elementFormat;
  private ByteBuf[] elementBuffers;
  private int[] dimensions;
  private Class<?> sourceArrayType;

  public PGBuffersArray(Context context, ArrayType type, FieldFormat elementFormat, ByteBuf[] elementBuffers, int[] dimensions) {
    this(context, type, elementFormat, elementBuffers, dimensions, null);
  }

  private PGBuffersArray(Context context, ArrayType type, FieldFormat elementFormat, ByteBuf[] elementBuffers, int[] dimensions, Class<?> sourceArrayType) {
    super(context, type);
    this.elementFormat = elementFormat;
    this.elementBuffers = elementBuffers;
    this.dimensions = dimensions;
    this.sourceArrayType = sourceArrayType;
  }

  public static PGBuffersArray encode(Context context, ArrayType type, Object array) throws IOException {

    int[] dimensions = getDimensions(array);
    int totalElements =  strideOfDimensions(dimensions);
    Type componentType = type.getElementType();
    FieldFormat format = type.getElementType().getResultFormat();
    List<ByteBuf> elementBuffers = new ArrayList<>(totalElements);

    encode(context, componentType, format, array, elementBuffers);

    return new PGBuffersArray(context, type, format, elementBuffers.toArray(EMPTY_BUFFERS), dimensions, array.getClass());
  }

  public static void encode(Context context, Type componentType, FieldFormat format, Object value, List<ByteBuf> elementBuffers) throws IOException {

    if (value.getClass().isArray()) {

      int length = Array.getLength(value);
      for (int elementIdx = 0; elementIdx < length; ++elementIdx) {

        Object element = Array.get(value, elementIdx);
        if (element == null) {
          elementBuffers.add(null);
          continue;
        }

        encode(context, componentType, format, element, elementBuffers);
      }

    }
    else {

      ByteBufAllocator byteBufAllocator = context.getAllocator();

      ByteBuf valueBuffer = null;

      switch (format) {
        case Text: {
          StringBuilder elementBuffer = new StringBuilder();
          componentType.getTextCodec().getEncoder()
              .encode(context, componentType, value, null, elementBuffer);

          valueBuffer = ByteBufUtil.writeUtf8(byteBufAllocator, elementBuffer);
        }
        break;

        case Binary: {
          valueBuffer = byteBufAllocator.buffer();
          componentType.getBinaryCodec().getEncoder()
              .encode(context, componentType, value, null, valueBuffer);
        }
        break;
      }

      elementBuffers.add(valueBuffer);

    }

  }

  public int getLength() {
    if (dimensions.length == 0) return  0;
    return dimensions[0];
  }

  @Override
  protected Object getArray(Context context, Class<?> targetComponentType, long index, int count) throws SQLException {

    int offset = (int) index - 1;

    if (offset < 0 || (offset + count) > getLength()) {
      throw new SQLException("Invalid array slice");
    }

    if (targetComponentType == null) {
      Type elementType = type.getElementType();
      Class<?> elementClass =
          sourceArrayType != null ?
              getElementType(sourceArrayType) :
              elementType.getCodec(elementFormat).getDecoder().getDefaultClass();
      targetComponentType = lookupCustomType(elementType, context.getCustomTypeMap(), elementClass);
    }

    try {
      return getArray(context, targetComponentType, dimensions, offset, count);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  private Object getArray(Context context, Class<?> targetComponentType, int[] dimensions, int offset, int count) throws IOException {

    Object result;

    if (dimensions.length > 1) {

      // Handle arrays of arrays

      dimensions = dimensions.clone();
      dimensions[0] = count;

      result = Array.newInstance(targetComponentType, dimensions);

      int[] subDimensions = copyOfRange(dimensions, 1, dimensions.length);
      int stride = strideOfDimensions(subDimensions);

      for (int c = 0; c < count; ++c) {
        Object subArray = getArray(context, targetComponentType, subDimensions, offset + c * stride, subDimensions[0]);
        Array.set(result, c, subArray);
      }

    }
    else {

      // Handle arrays of elements

      Type componentType = type.getElementType();
      result = Array.newInstance(targetComponentType, count);

      targetComponentType = boxType(targetComponentType);

      for (int c = 0; c < count; ++c) {
        Object element = null;

        ByteBuf elementBuffer = elementBuffers[offset + c];
        if (elementBuffer != null) {
          switch (elementFormat) {
            case Text: {
              String elementStr = elementBuffer.toString(UTF_8);
              element = componentType.getTextCodec().getDecoder()
                  .decode(context, componentType, componentType.getLength(), null, elementStr, targetComponentType, null);
            }
            break;

            case Binary: {
              element = componentType.getBinaryCodec().getDecoder()
                  .decode(context, componentType, componentType.getLength(), null, elementBuffer, targetComponentType, null);
            }
            break;
          }
          elementBuffer.resetReaderIndex();
        }

        Array.set(result, c, element);
      }

    }

    return result;
  }

  @Override
  protected ResultSet getResultSet(Context context, long index, int count) throws SQLException {

    int offset = (int) index - 1;

    if (offset < 0 || (offset + count) > getLength()) {
      throw new SQLException("Invalid array slice");
    }

    ByteBufAllocator byteBufAllocator = context.getAllocator();
    Registry reg = context.getRegistry();

    int stride = strideOfDimensions(dimensions, 1);

    FieldFormat elementFormat;
    Type elementType;
    if (stride > 1) {
      elementType = new NestedArrayType(type, type.getElementType(), this.elementFormat, copyOfRange(dimensions, 1, dimensions.length));
      elementFormat = FieldFormat.Binary;
    }
    else {
      elementType = type.getElementType();
      elementFormat = this.elementFormat;
    }

    ResultField[] fields = new ResultField[] {
        new ResultField("INDEX", 0, (short) 0, reg.loadBaseType("int4"), (short) 0, 0, FieldFormat.Binary),
        new ResultField("VALUE", 0, (short) 0, elementType, (short) 0, 0, elementFormat)
    };

    RowDataSet results = new RowDataSet(count);
    for (int c = 0; c < count; ++c) {

      ByteBuf indexBuffer = byteBufAllocator.buffer(4).writeInt(offset + c + 1);

      ByteBuf elementBuffer;
      if (stride > 1) {
        CompositeByteBuf compElementBuffer = byteBufAllocator.compositeBuffer(stride);
        for (int s = 0; s < stride; ++s) {
          compElementBuffer.addComponent(elementBuffers[((offset + c) * stride) + s].retainedDuplicate());
        }
        elementBuffer = compElementBuffer;
      }
      else {
        elementBuffer = elementBuffers[offset + c].retainedDuplicate();
      }

      results.add(new FieldBuffersRowData(new ByteBuf[] {indexBuffer, elementBuffer}, context.getAllocator()));
    }

    PGDirectConnection connection = (PGDirectConnection) context.unwrap();
    PGStatement stmt = connection.createStatement();
    stmt.closeOnCompletion();
    return stmt.createResultSet(fields, results, true, context.getCustomTypeMap());
  }

  @Override
  public void free() {
    if (elementBuffers != null) {
      for (ByteBuf elementBuf : elementBuffers) {
        ReferenceCountUtil.release(elementBuf);
      }
    }
    this.context = null;
    this.type = null;
    this.elementFormat = null;
    this.dimensions = null;
    this.sourceArrayType = null;
    this.elementBuffers = null;
  }

}
