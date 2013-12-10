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

import com.impossibl.postgres.data.Path;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.GeometryParsers;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author croudet
 *
 * @version $Revision:  $, $Date: $, $Name: $, $Author: $
 */
public class Polygons extends SimpleProcProvider {

  public Polygons() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "poly_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Polygon;
    }

    @Override
    public Class<?> getOutputType() {
      return double[][].class;
    }

    @Override
    public  double[][] decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length < 20) {
        // at least one point
        throw new IOException("invalid length " + length);
      }
      int npts = buffer.readInt();
      if (npts <= 0 || npts >= Integer.MAX_VALUE) {
        throw new IOException("invalid number of points in external \"polygon\" value");
      }
      double[][] points = new double[npts][];
      for (int i = 0; i < npts; ++i) {
        double[] point = new double[2];
        point[0] = buffer.readDouble();
        point[1] = buffer.readDouble();
        points[i] = point;
      }
      return points;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return double[][].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Polygon;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
      if (val == null) {
        buffer.writeInt(-1);
      }
      else {
        double[][] points = (double[][]) val;
        // int + npts * 2 points
        int size = 4 + (points.length * 8 * 2);
        buffer.writeInt(size);
        buffer.writeInt(points.length);
        for (int i = 0; i < points.length; ++i) {
          double[] point = points[i];
          buffer.writeDouble(point[0]);
          buffer.writeDouble(point[1]);
        }
      }
    }
  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Polygon;
    }

    @Override
    public Class<?> getOutputType() {
      return double[][].class;
    }

    @Override
    public double[][] decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
      return GeometryParsers.INSTANCE.parsePolygon(buffer);
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Path.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Polygon;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      if (val == null) {
        return;
      }
      buffer.append(new Path((double[][]) val, true));
    }

  }

}
