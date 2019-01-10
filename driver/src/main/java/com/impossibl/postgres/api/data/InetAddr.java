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
package com.impossibl.postgres.api.data;

import static com.impossibl.postgres.api.data.InetAddr.Family.IPv4;
import static com.impossibl.postgres.api.data.InetAddr.Family.IPv6;
import static com.impossibl.postgres.utils.guava.Preconditions.checkArgument;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.BitSet;



public class InetAddr {

  private static final String TOO_MANY_WORDS = "too many words";
  private static final String INVALID_ADDRESS = "invalid address";

  public enum Family {

    IPv4(4, 4), IPv6(6, 16);

    private int byteSize;
    private int version;

    Family(int version, int byteSize) {
      this.version = version;
      this.byteSize = byteSize;
    }

    public int getByteSize() {
      return byteSize;
    }

    public int getVersion() {
      return version;
    }

  }

  protected byte[] address;
  protected short maskBits;

  protected InetAddr(Object[] parts) {
    this((byte[]) parts[0], (short) parts[1]);
  }

  public InetAddr(byte[] address, short maskBits) {
    checkArgument(address.length == 4 || address.length == 16, "invalid address size");
    checkArgument(maskBits <= (address.length * 8), "invalid mask bits");
    this.address = address;
    this.maskBits = maskBits;
  }

  public InetAddr(String cidrAddress) throws IllegalArgumentException {
    this(parseN(cidrAddress, true));
  }

  public Family getFamily() {
    return address.length == 4 ? Family.IPv4 : Family.IPv6;
  }

  public byte[] getAddress() {
    return address;
  }

  public void setAddress(byte[] address) {
    this.address = address;
  }

  public byte[] getMaskAddress() {
    BitSet mask = new BitSet(address.length * 8);
    mask.set(0, maskBits);
    return mask.toByteArray();
  }

  public int getMaskBits() {
    return maskBits;
  }

  public void setMaskBits(short maskBits) {
    this.maskBits = maskBits;
  }

  public static InetAddr parseInetAddr(String inetAddr) {
    return parseInetAddr(inetAddr, false);
  }

  public static InetAddr parseInetAddr(String inetAddr, boolean allowShorthandNotation) {
    return parseInetAddr(inetAddr, allowShorthandNotation, inetAddr.indexOf(':') != -1 ? Family.IPv6 : Family.IPv4);
  }

  public static InetAddr parseInetAddr(String inetAddr, boolean allowShortNotation, Family family) {
    return new InetAddr(parseN(inetAddr, allowShortNotation, family));
  }

  protected static Object[] parseN(String inetAddr, boolean allowShortNotation) {
    return parseN(inetAddr, allowShortNotation, inetAddr.indexOf(':') != -1 ? Family.IPv6 : Family.IPv4);
  }

  protected static Object[] parseN(String inetAddr, boolean allowShortNotation, Family family) {

    switch (family) {
      case IPv6:
        return parse6(inetAddr);
      case IPv4:
        return parse4(inetAddr, allowShortNotation);

      default:
        throw new IllegalArgumentException("unknown family");
    }

  }

  private static Object[] parse4(String ipv4Addr, boolean allowShortNotation) throws IllegalArgumentException {

    if ((ipv4Addr == null) || (ipv4Addr.isEmpty())) {
      throw new IllegalArgumentException(INVALID_ADDRESS);
    }

    byte[] dst = new byte[IPv4.getByteSize()];
    short maskBits = 32;

    char[] srcb = ipv4Addr.toCharArray();
    boolean sawDigit = false;

    int octets = 0;
    int i = 0;
    char ch;
    int cur = 0;
    while (i < srcb.length) {

      ch = srcb[i++];

      if (Character.isDigit(ch)) {

        // note that Java byte is signed, so need to convert to int
        int sum = (dst[cur] & 0xff) * 10 + (Character.digit(ch, 10) & 0xff);
        if (sum > 255) {
          throw new IllegalArgumentException("octet is larger than 255");
        }

        dst[cur] = (byte) (sum & 0xff);

        if (!sawDigit) {
          if (++octets > IPv4.getByteSize()) {
            throw new IllegalArgumentException("too many octets");
          }
          sawDigit = true;
        }

      }
      else if (ch == '.' && sawDigit) {

        if (octets == IPv4.getByteSize()) {
          throw new IllegalArgumentException("too many octets");
        }

        cur++;
        dst[cur] = 0;
        sawDigit = false;
      }
      else if (ch == '/') {

        maskBits = 0;

        // Sum up mask bits
        while (i < srcb.length) {
          ch = srcb[i++];
          int sum = (maskBits & 0xff) * 10 + (Character.digit(ch, 10) & 0xff);
          if (sum > 32) {
            throw new IllegalArgumentException("mask is larger than 32");
          }
          maskBits = (short) sum;
        }

      }
      else {
        throw new IllegalArgumentException(INVALID_ADDRESS);
      }
    }

    if (octets < IPv4.getByteSize() && !allowShortNotation) {
      throw new IllegalArgumentException("invalid # of octets");
    }

    return new Object[] {dst, maskBits};
  }

  private static void format4(byte[] src, int offset, StringBuilder out) {
    out.append(src[offset] & 0xFF);
    out.append('.');
    out.append(src[offset + 1] & 0xFF);
    out.append('.');
    out.append(src[offset + 2] & 0xFF);
    out.append('.');
    out.append(src[offset + 3] & 0xFF);
  }

  private static Object[] parse6(String ipv6Addr) throws IllegalArgumentException {

    // Shortest valid string is "::", hence at least 2 chars
    if (ipv6Addr == null || ipv6Addr.length() < 2) {
      throw new IllegalArgumentException("invalid length");
    }

    char[] srcb = ipv6Addr.toCharArray();
    int srcbLength = srcb.length;

    byte[] dst = new byte[IPv6.getByteSize()];
    short maskBits = 128;

    int pc = ipv6Addr.indexOf('%');
    if (pc == srcbLength - 1) {
      throw new IllegalArgumentException(INVALID_ADDRESS);
    }

    if (pc != -1) {
      srcbLength = pc;
    }

    int i = 0, j = 0;
    /* Leading :: requires some special handling. */
    if (srcb[i] == ':') {
      if (srcb[++i] != ':') {
        throw new IllegalArgumentException("invalid prefix");
      }
    }

    int colonp = -1;
    int curtok = i;
    boolean sawXDigit = false;
    int val = 0;
    char ch;

    while (i < srcbLength) {

      ch = srcb[i++];

      int chval = Character.digit(ch, 16);
      if (chval != -1) {
        val <<= 4;
        val |= chval;
        if (val > 0xffff) {
          throw new IllegalArgumentException("word value too large");
        }
        sawXDigit = true;
        continue;
      }

      if (ch == ':') {

        curtok = i;

        if (!sawXDigit) {
          if (colonp != -1) {
            throw new IllegalArgumentException(INVALID_ADDRESS);
          }
          colonp = j;
          continue;
        }
        else if (i == srcbLength) {
          throw new IllegalArgumentException(INVALID_ADDRESS);
        }

        if (j + 2 > IPv6.getByteSize()) {
          throw new IllegalArgumentException(TOO_MANY_WORDS);
        }

        dst[j++] = (byte) ((val >> 8) & 0xff);
        dst[j++] = (byte) (val & 0xff);
        sawXDigit = false;
        val = 0;

        continue;
      }

      if (ch == '.' && ((j + IPv4.getByteSize()) <= IPv6.getByteSize())) {

        int endtok;
        for (endtok = curtok; endtok < srcbLength; ++endtok)
          if (ipv6Addr.charAt(endtok) == '/') break;

        String ipv4AddrEmb = ipv6Addr.substring(curtok, endtok);

        Object[] ipv4parts;
        try {
          ipv4parts = parse4(ipv4AddrEmb, false);
        }
        catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("invalid embedded IPv4 address");
        }

        byte[] ipv4Addr = (byte[]) ipv4parts[0];
        for (int k = 0; k < IPv4.getByteSize(); k++) {
          dst[j++] = ipv4Addr[k];
        }

        sawXDigit = false;

        break;
      }

      if (ch == '/') {

        maskBits = 0;

        // Sum up mask bits
        while (i < srcb.length) {
          ch = srcb[i++];
          int sum = (maskBits & 0xff) * 10 + (Character.digit(ch, 10) & 0xff);
          if (sum > 128) {
            throw new IllegalArgumentException("mask is larger than 128");
          }
          maskBits = (short) sum;
        }

        break;
      }

      throw new IllegalArgumentException(INVALID_ADDRESS);
    }

    if (sawXDigit) {

      if (j + 2 > IPv6.getByteSize()) {
        throw new IllegalArgumentException(TOO_MANY_WORDS);
      }

      dst[j++] = (byte) ((val >> 8) & 0xff);
      dst[j++] = (byte) (val & 0xff);
    }

    if (colonp != -1) {

      int n = j - colonp;
      if (j == IPv6.getByteSize()) {
        throw new IllegalArgumentException(TOO_MANY_WORDS);
      }

      for (i = 1; i <= n; i++) {
        dst[IPv6.getByteSize() - i] = dst[colonp + n - i];
        dst[colonp + n - i] = 0;
      }

      j = IPv6.getByteSize();
    }

    if (j != IPv6.getByteSize()) {
      throw new IllegalArgumentException("invalid format");
    }

    return new Object[] {dst, maskBits};
  }

  private static void format6(byte[] src, StringBuilder out) {

    final boolean embeddedInet4 = src[0] == 0 && src[1] == 0 && src[2] == 0 && src[3] == 0 && src[4] == 0;
    final int size = embeddedInet4 ? (IPv6.getByteSize() - IPv4.getByteSize()) / 2 : IPv6.getByteSize() / 2;

    for (int i = 0; i < size; i++) {
      out.append(Integer.toHexString(((src[i << 1] << 8) & 0xff00) | (src[(i << 1) + 1] & 0xff)));
      if (i < size - 1) {
        out.append(':');
      }
    }

    if (embeddedInet4) {
      out.append(':');
      format4(src, 12, out);
    }

  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(address);
    result = prime * result + maskBits;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InetAddr other = (InetAddr) obj;
    if (!Arrays.equals(address, other.address))
      return false;
    if (maskBits != other.maskBits)
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();

    if (address.length == 4)
      format4(address, 0, out);
    else
      format6(address, out);

    if (maskBits != (address.length * 8))
      out.append('/').append(maskBits);

    return out.toString();
  }

  public InetAddress toInetAddress() {
    try {
      return InetAddress.getByAddress(address);
    }
    catch (UnknownHostException e) {
      // Should never happen...
      throw new RuntimeException(e);
    }
  }

}
