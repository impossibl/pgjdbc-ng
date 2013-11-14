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


import com.impossibl.postgres.data.Inet;
import com.impossibl.postgres.data.Inet.Family;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author croudet
 *
 * @version $Revision:  $, $Date: $, $Name: $, $Author: $
 */
public class Inets extends SimpleProcProvider {
  private static final short PGSQL_AF_INET = 2;
  private static final short PGSQL_AF_INET6 = 3;

  // http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/utils/inet.h;h=3d8e31c31c83d5544ea170144b03b0357cd77b2b;hb=HEAD
  public Inets() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "inet_");
  }

  static class BinDecoder extends BinaryDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return Inet.class;
    }

    @Override
    public Inet decode(Type type, ChannelBuffer buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      }
      // length should be 8 or 20
      short family = buffer.readUnsignedByte();
      short mask = buffer.readUnsignedByte();
      int addrSize = buffer.readUnsignedShort();
      if (family == PGSQL_AF_INET) {
        if (addrSize != 4) {
          throw new IOException("Invalid inet4 size: " + addrSize);
        }
      }
      else if (family == PGSQL_AF_INET6) {
        if (addrSize != 16) {
          throw new IOException("Invalid inet6 size: " + addrSize);
        }
      }
      else {
        throw new IOException("Invalid inet family: " + family);
      }
      byte[] addr = new byte[addrSize];
      buffer.readBytes(addr);
      return new Inet(addr, mask);
    }

  }

  static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return Inet.class;
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
        Inet inet = (Inet) val;
        buffer.writeInt(inet.getFamily() == Family.IPV4 ? 8 : 20);
        buffer.writeByte(inet.getFamily() == Family.IPV4 ? PGSQL_AF_INET : PGSQL_AF_INET6);
        buffer.writeByte(inet.getNetmask());
        buffer.writeShort(inet.getFamily() == Family.IPV4 ? 4 : 16);
        buffer.writeBytes(inet.getAddress());
      }
    }
  }

  static class TxtDecoder extends TextDecoder {

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return Inet.class;
    }

    @Override
    public Inet decode(Type type, CharSequence buffer, Context context) throws IOException {
      try {
        return new Inet(buffer.toString());
      }
      catch (RuntimeException ex) {
        throw new IOException(ex);
      }
    }

  }

  static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return Inet.class;
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      Inet inet = (Inet) val;
      buffer.append(inet.toString());
    }

  }

}
