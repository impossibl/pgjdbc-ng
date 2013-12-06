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
public class LSegs extends SimpleProcProvider {

  interface Formatter {
    String getLeftDelim();

    String getRightDelim();

    double[] parse(CharSequence buffer);

    //double[] reorder(double[] p);
  }

  static class LSegFormatter implements Formatter {

    @Override
    public String getLeftDelim() {
      return "[";
    }

    @Override
    public String getRightDelim() {
      return "]";
    }

    @Override
    public double[] parse(CharSequence buffer) {
      return GeometryParsers.INSTANCE.parseLSeg(buffer);
    }

//    @Override
//    public double[] reorder(double[] p) {
//      return p;
//    }

  }

  public LSegs() {
    this("lseg_", new LSegFormatter());
  }

  public LSegs(String pgtype, Formatter formatter) {
    super(new TxtEncoder(formatter), new TxtDecoder(formatter), new BinEncoder(), new BinDecoder(), pgtype);
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return double[].class;
    }

    @Override
    public double[] decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 32) {
        throw new IOException("invalid length " + length);
      }
      // phigh.x, phigh.y, plow.x, plow.y
      return new double[] {buffer.readDouble(), buffer.readDouble(), buffer.readDouble(), buffer.readDouble()};
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return double[].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
      if (val == null) {
        buffer.writeInt(-1);
      }
      else {
        double[] box = (double[]) val;
        if (box.length != 4) {
          throw new IOException("invalid length");
        }
        buffer.writeInt(32);
        buffer.writeDouble(box[0]); // phigh.x
        buffer.writeDouble(box[1]); // phigh.y
        buffer.writeDouble(box[2]); // plow.x
        buffer.writeDouble(box[3]); // plow.y
      }
    }
  }

  static class TxtDecoder extends TextDecoder {
    private Formatter formatter;

    TxtDecoder(Formatter f) {
      formatter = f;
    }

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return double[].class;
    }

    @Override
    public double[] decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
      return formatter.parse(buffer);
    }

  }

  static class TxtEncoder extends TextEncoder {
    private Formatter formatter;

    TxtEncoder(Formatter f) {
      formatter = f;
    }

    @Override
    public Class<?> getInputType() {
      return double[].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      double[] lseg = (double[]) val;
      if (lseg.length != 4) {
        throw new IOException("invalid length");
      }
      buffer.append(formatter.getLeftDelim()).append('(').append(Double.toString(lseg[0])).append(',').append(Double.toString(lseg[1])).append("),(").append(Double.toString(lseg[2])).append(',').append(Double.toString(lseg[3])).append(')').append(formatter.getRightDelim());
    }

  }
}
