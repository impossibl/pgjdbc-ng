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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffer;

public class MacAddrs extends SimpleProcProvider {

  // http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/utils/inet.h;h=3d8e31c31c83d5544ea170144b03b0357cd77b2b;hb=HEAD
  // http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/utils/adt/mac.c;h=aa9993fa5c6406fa7274ad61de270d5086781a5d;hb=HEAD
  public MacAddrs() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "macaddr_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return byte[].class;
    }

    @Override
    public byte[] decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      else if (length != 6) {
        throw new IOException("invalid length");
      }
      byte[] bytes = new byte[6];
      buffer.readBytes(bytes);
      return bytes;
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return byte[].class;
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
        byte[] bytes = (byte[]) val;
        if (bytes.length != 6) {
          throw new IOException("invalid length");
        }
        buffer.writeInt(6);
        buffer.writeBytes(bytes);
      }
    }
  }

  static class TxtDecoder extends TextDecoder {
    /*
     * '08:00:2b:01:02:03' '08-00-2b-01-02-03' '08002b:010203' '08002b-010203'
     * '0800.2b01.0203' '08002b010203'
     */
    private static final Pattern macPattern = Pattern
        .compile("([0-9a-f-A-F]{2})[:-]?([0-9a-f-A-F]{2})[-:.]?([0-9a-f-A-F]{2})[:-]?([0-9a-f-A-F]{2})[-:.]?([0-9a-f-A-F]{2})[:-]?([0-9a-f-A-F]{2})");

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return byte[].class;
    }

    @Override
    public byte[] decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
      Matcher m = macPattern.matcher(buffer);
      if (!m.matches()) {
        throw new IOException("Invalid Mac address: " + buffer);
      }
      byte[] addr = new byte[6];
      for (int i = 0; i < 6; i++) {
        addr[i] = (byte) Integer.parseInt(m.group(i + 1), 16);
      }
      return addr;
    }

  }

  static class TxtEncoder extends TextEncoder {
    private static final char[] hexDigits = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    private static final char separator = ':';

    @Override
    public Class<?> getInputType() {
      return byte[].class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      byte[] addr = (byte[]) val;
      if (addr.length != 6) {
        throw new IOException("invalid length");
      }
      for (byte b : addr) {
        int bi = b & 0xff;
        buffer.append(hexDigits[bi >> 4]);
        buffer.append(hexDigits[bi & 0xf]).append(separator);
      }
      buffer.setLength(buffer.length() - 1);
    }

  }

}
