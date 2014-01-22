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

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public Class<?> getOutputType() {
      return Path.class;
    }

    @Override
    public Path decode(Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length < 21) {
        // at least one point
        throw new IOException("invalid length " + length);
      }
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

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Path.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public void encode(Type type, ByteBuf buffer, Object val, Context context) throws IOException {
      if (val == null) {
        buffer.writeInt(-1);
      }
      else {
        Path path = (Path) val;
        double[][] points = path.getPoints();
        // byte + int + npts * 2 points
        int size = 1 + 4 + (points == null ? 0 : points.length * 8 * 2);
        buffer.writeInt(size);
        buffer.writeByte(path.isClosed() ? 1 : 0);
        buffer.writeInt(points == null ? 0 : points.length);
        if (points != null) {
          for (int i = 0; i < points.length; ++i) {
            double[] point = points[i];
            buffer.writeDouble(point[0]);
            buffer.writeDouble(point[1]);
          }
        }
      }
    }
  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public Class<?> getOutputType() {
      return Path.class;
    }

    @Override
    public Path decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
      return GeometryParsers.INSTANCE.parsePath(buffer);
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Path.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Path;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      if (val == null) {
        return;
      }
      buffer.append(((Path) val).toString());
    }

  }
}
