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

import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.InetAddr.Family;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Type;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

abstract class Networks extends SimpleProcProvider {

  enum Kind {
    Inet,
    Cidr,
  }

  private static final short PGSQL_AF_INET = 2;
  private static final short PGSQL_AF_INET6 = 3;

  interface NetworkObjectFactory<T extends InetAddr> {

    T newNetworkObject(byte[] addr, short netmask);

    T newNetworkObject(String v);

    Kind getKind();

    Class<T> getObjectType();

  }

  private static InetAddr convertInput(Type type, Object value) throws ConversionException {

    if (value instanceof InetAddr) {
      return (InetAddr) value;
    }

    if (value instanceof String) {
      return InetAddr.parseInetAddr((String) value, true);
    }

    throw new ConversionException(value.getClass(), type);
  }

  private static Object convertOutput(Type type, InetAddr value, Kind kind, Class<?> targetClass) throws ConversionException {

    if (targetClass == InetAddr.class && kind == Kind.Inet) {
      return value;
    }

    if (targetClass == CidrAddr.class && kind == Kind.Cidr) {
      return value;
    }

    if (targetClass == String.class) {
      return value.toString();
    }

    throw new ConversionException(type, targetClass);
  }

  // http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/utils/inet.h;h=3d8e31c31c83d5544ea170144b03b0357cd77b2b;hb=HEAD
  Networks(String pgtype, NetworkObjectFactory<? extends InetAddr> nof) {
    super(new TxtEncoder(), new TxtDecoder(nof), new BinEncoder(), new BinDecoder(nof), pgtype);
  }

  static class BinDecoder extends BaseBinaryDecoder {

    private NetworkObjectFactory<? extends InetAddr> nof;

    BinDecoder(NetworkObjectFactory<? extends InetAddr> nof) {
      this.nof = nof;
    }

    @Override
    public Class<?> getDefaultClass() {
      return nof.getObjectType();
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      if (length != 8 && length != 20) {
        throw new IOException("Invalid length: " + length);
      }

      // family, bits, is_cidr, address length, address in network byte order.
      short family = buffer.readUnsignedByte();
      short mask = buffer.readUnsignedByte();

      /* byte isCidr = */
      buffer.readByte();
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

      InetAddr value = nof.newNetworkObject(addr, mask);

      return convertOutput(type, value, nof.getKind(), targetClass);
    }

  }

  static class BinEncoder extends BaseBinaryEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      InetAddr inet = convertInput(type, value);

      byte[] addr = inet.getAddress();
      boolean ipV4 = inet.getFamily() == Family.IPv4;
      buffer.writeByte(ipV4 ? PGSQL_AF_INET : PGSQL_AF_INET6);
      buffer.writeByte(inet.getMaskBits());
      buffer.writeByte(type.getName().equals("cidr") ? 0 : 1);
      buffer.writeByte(addr.length);
      buffer.writeBytes(addr);
    }

  }

  static class TxtDecoder extends BaseTextDecoder {

    private NetworkObjectFactory<? extends InetAddr> nof;

    TxtDecoder(NetworkObjectFactory<? extends InetAddr> nof) {
      this.nof = nof;
    }

    @Override
    public Class<?> getDefaultClass() {
      return nof.getObjectType();
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      InetAddr value = nof.newNetworkObject(buffer.toString());

      return convertOutput(type, value, nof.getKind(), targetClass);
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      InetAddr inet = convertInput(type, value);

      buffer.append(inet.toString());
    }

  }

}
