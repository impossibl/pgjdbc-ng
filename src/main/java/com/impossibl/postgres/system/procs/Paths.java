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

import com.impossibl.postgres.api.data.Path;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.GeometryParsers;

import java.io.IOException;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;

/**
 * @author croudet
 *
 * @version $Revision:  $, $Date: $, $Name: $, $Author: $
 */
public class Paths extends SimpleProcProvider {

  public Paths() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "path_");
  }

  static class BinDecoder extends BaseBinaryDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Path.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      boolean closed = buffer.readByte() != 0;
      int npts = buffer.readInt();
      if (npts <= 0 || npts >= Integer.MAX_VALUE) {
        throw new IOException("invalid number of points in external \"path\" value");
      }

      double[][] points = new double[npts][];
      for (int i = 0; i < npts; ++i) {
        double[] point = new double[2];
        point[0] = buffer.readDouble();
        point[1] = buffer.readDouble();
        points[i] = point;
      }

      return new Path(points, closed);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Path path = (Path) value;
      double[][] points = path.getPoints();

      buffer.writeByte(path.isClosed() ? 1 : 0);
      buffer.writeInt(points == null ? 0 : points.length);
      if (points != null) {
        for (double[] point : points) {
          buffer.writeDouble(point[0]);
          buffer.writeDouble(point[1]);
        }
      }
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Path.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {
      return GeometryParsers.INSTANCE.parsePath(buffer);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(value.toString());
    }

  }
}
