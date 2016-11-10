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
import java.util.List;

import static java.lang.Math.max;

import io.netty.buffer.ByteBuf;


public class BufferedDataRow implements DataRow {

  private ByteBuf buffer;
  private List<ResultField> columns;
  private int[] columnOffsets;
  private Context parsingContext;

  private BufferedDataRow(ByteBuf buffer, List<ResultField> columns, int[] columnOffsets, Context parsingContext) {
    this.buffer = buffer.retain();
    this.columns = columns;
    this.columnOffsets = columnOffsets;
    this.parsingContext = parsingContext;
  }

  public static BufferedDataRow parse(ByteBuf buffer, List<ResultField> columns, Context parsingContext) {

    int columnsCount = buffer.readUnsignedShort();
    int[] offsets = new int[columnsCount];

    for (int c = 0; c < columnsCount; ++c) {
      offsets[c] = buffer.readerIndex();
      buffer.skipBytes(max(buffer.readInt(), 0));
    }

    return new BufferedDataRow(buffer, columns, offsets, parsingContext);
  }

  @Override
  public Object getColumn(int columnIndex) throws IOException {

    ResultField field = columns.get(columnIndex);
    Type type = field.typeRef.get();
    int offset = columnOffsets[columnIndex];
    ByteBuf fieldBuffer = buffer.slice(offset, max(buffer.getInt(offset), 0) + 4);

    return type.getCodec(field.format).decoder.decode(type, field.typeLength, field.typeModifier, fieldBuffer, parsingContext);
  }

  @Override
  public void release() {
    buffer.release();
  }

}
