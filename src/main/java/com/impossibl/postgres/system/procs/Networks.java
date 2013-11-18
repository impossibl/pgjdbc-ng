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

import com.impossibl.postgres.data.NetworkBase;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

abstract class Networks extends SimpleProcProvider {
  private static final short PGSQL_AF_INET = 2;
  private static final short PGSQL_AF_INET6 = 3;

  interface NetworkObjectFactory<T extends NetworkBase> {
    T newNetworkObject(byte[] addr, short netmask);

    T newNetworkObject(String v);
 
    Class<? extends NetworkBase> objectClass();
  }

  // http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/utils/inet.h;h=3d8e31c31c83d5544ea170144b03b0357cd77b2b;hb=HEAD
  public Networks(String pgtype, NetworkObjectFactory<? extends NetworkBase> nof) {
    super(new TxtEncoder(nof), new TxtDecoder(nof), new BinEncoder(nof), new BinDecoder(nof), pgtype);
  }

  static class BinDecoder extends BinaryDecoder {
    private NetworkObjectFactory<? extends NetworkBase> nof;

    public BinDecoder(NetworkObjectFactory<? extends NetworkBase> nof) {
      this.nof = nof;
    }

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return nof.objectClass();
    }

    @Override
    public NetworkBase decode(Type type, Short typeLength, Integer typeModifier, ChannelBuffer buffer, Context context) throws IOException {
      int length = buffer.readInt();
      if (length == -1) {
        return null;
      } // length should be 8 or 20
      else if (length != 8 && length != 20) {
        throw new IOException("Invalid length: " + length);
      }
      // family, bits, is_cidr, address length, address in network byte order.
      short family = buffer.readUnsignedByte();
      short mask = buffer.readUnsignedByte();
      // is_cidr 0 for inet and 1 for cidr
      buffer.skipBytes(1);
      // System.out.println(buffer.readByte());
      int addrSize = buffer.readUnsignedByte();
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
      return nof.newNetworkObject(addr, mask);
    }

  }

  static class BinEncoder extends BinaryEncoder {
    private NetworkObjectFactory<? extends NetworkBase> nof;

    public BinEncoder(NetworkObjectFactory<? extends NetworkBase> nof) {
      this.nof = nof;
    }

    @Override
    public Class<?> getInputType() {
      return nof.objectClass();
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
        NetworkBase inet = (NetworkBase) val;
        byte[] addr = inet.getAddress();
        boolean ipV4 = addr.length == NetworkBase.IPv4INADDRSZ;
        buffer.writeInt(ipV4 ? 8 : 20);
        buffer.writeByte(ipV4 ? PGSQL_AF_INET : PGSQL_AF_INET6);
        buffer.writeByte(inet.getNetmask());
        // 869 inet - 650 cidr
        buffer.writeByte(type.getId() == 869 ? 0 : 1);
        buffer.writeByte(ipV4 ? NetworkBase.IPv4INADDRSZ : NetworkBase.IPv6INADDRSZ);
        buffer.writeBytes(addr);
      }
    }
  }

  static class TxtDecoder extends TextDecoder {
    private NetworkObjectFactory<? extends NetworkBase> nof;

    public TxtDecoder(NetworkObjectFactory<? extends NetworkBase> nof) {
      this.nof = nof;
    }

    @Override
    public PrimitiveType getInputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public Class<?> getOutputType() {
      return nof.objectClass();
    }

    @Override
    public NetworkBase decode(Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Context context) throws IOException {
      try {
        return nof.newNetworkObject(buffer.toString());
      }
      catch (RuntimeException ex) {
        throw new IOException(ex);
      }
    }

  }

  static class TxtEncoder extends TextEncoder {
    private NetworkObjectFactory<? extends NetworkBase> nof;

    public TxtEncoder(NetworkObjectFactory<? extends NetworkBase> nof) {
      this.nof = nof;
    }

    @Override
    public Class<?> getInputType() {
      return nof.objectClass();
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return PrimitiveType.Binary;
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
      NetworkBase inet = (NetworkBase) val;
      buffer.append(inet.toString());
    }

  }

}
