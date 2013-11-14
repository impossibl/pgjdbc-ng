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
package com.impossibl.postgres.data;

import java.util.Arrays;

public class Inet {
  private Family family;
  private short netmask;
  private byte[] addr;

  private static final int IPv4INADDRSZ = 4;
  private static final int IPv6INADDRSZ = 16;
  private static final int INT16SZ = 2;

  public enum Family {
    IPV4, IPV6
  }

  public Inet(String address) {
    setAddress(address);
  }

  public Inet(byte[] bytes) {
    setAddress(bytes);
    netmask = (short) (bytes.length == IPv4INADDRSZ ? 32 : 128);
  }

  public Inet(byte[] bytes, short mask) {
    setAddress(bytes);
    netmask = mask;
  }

  public Family getFamily() {
    return family;
  }

  public short getNetmask() {
    return netmask;
  }

  public byte[] getAddress() {
    return addr;
  }

  private void setAddress(byte[] bytes) throws IllegalArgumentException {
    if ((bytes == null) || ((bytes.length != IPv4INADDRSZ) && (bytes.length != IPv6INADDRSZ))) {
      throw new IllegalArgumentException("Invalid byte array for Inet "
          + (bytes == null ? "null" : bytes.length + ""));
    }
    family = bytes.length == IPv4INADDRSZ ? Family.IPV4 : Family.IPV6;
    // should I duplicate the array?
    addr = bytes;
  }

//  public static void main(String... args) {
//    String[] inets = new String[] {
//        "192.168.2.3",
//        "2001:4f8:3:ba:2e0:81ff:fe22:d1f1",
//        "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/10",
//        "10.2.1.255/12",
//        "2001:4f8:3:ba::/64",
//        "2001:4f8:3:ba:0:0:0:0",
//        "255.255.255.255"
//    };
//    for (String inet: inets) {
//      System.out.println(inet + " " + new Inet(inet));
//    }
//  }

  private void setAddress(String addr) {
    int maskIdx = addr.indexOf('/');
    int maxMask;
    String ipaddr;
    if (maskIdx > 0) {
      netmask = Byte.parseByte(addr.substring(maskIdx + 1));
      ipaddr = addr.substring(0, maskIdx);
    }
    else {
      ipaddr = addr;
    }
    byte[] addrss;
    if (ipaddr.indexOf(':') == -1) {
      addrss = parseIPv4Address(ipaddr);
      family = Family.IPV4;
      maxMask = 32;
    }
    else {
      addrss = parseIPv6Address(ipaddr);
      family = Family.IPV6;
      maxMask = 128;
    }
    if (addrss == null) {
      throw new IllegalArgumentException("Invalid Inet: " + addr);
    }
    if (maskIdx == -1) {
      // set default mask
      if (family == Family.IPV4) {
        netmask = (short) 32;
      }
      else {
        netmask = (short) 128;
      }
    }
    else {
      if ((this.netmask < 0) || (this.netmask > maxMask)) {
        throw new IllegalArgumentException("Invalid inet mask: " + addr + " - mask: " + netmask);
      }
    }
    this.addr = addrss;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(addr);
    result = prime * result + ((family == null) ? 0 : family.hashCode());
    result = prime * result + netmask;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Inet))
      return false;
    Inet other = (Inet) obj;
    if (family != other.family)
      return false;
    if (netmask != other.netmask)
      return false;
    if (!Arrays.equals(addr, other.addr))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buffer;
    if (family == Family.IPV4) {
      buffer = toStringIPv4Address(addr);
    }
    else {
      buffer = toStringIPv6Address(addr);
    }
    if ((family == Family.IPV4 && netmask != 32) || (family == Family.IPV6 && netmask != 128)) {
      buffer.append('/').append(netmask);
    }
    return buffer.toString();
  }

  /**
   * Parses the object by decomposing the passed string into it four components.
   * The string must be in the format of "xxx.xxx.xxx.xxx" where xxx is in the
   * range of [0..256).
   * 
   * @param ipv4Addr
   *          The dotted decimal address.
   * @return The ip address as a byte array of size 4, or null if the given
   *         String is an invalid ipV4 address.
   */
  public static byte[] parseIPv4Address(String ipv4Addr) {
    if ((ipv4Addr == null) || (ipv4Addr.isEmpty())) {
      return null;
    }
    int octets;
    char ch;
    byte[] dst = new byte[IPv4INADDRSZ];
    char[] srcb = ipv4Addr.toCharArray();
    boolean saw_digit = false;
    octets = 0;
    int i = 0;
    int cur = 0;
    while (i < srcb.length) {
      ch = srcb[i++];
      if (Character.isDigit(ch)) {
        // note that Java byte is signed, so need to convert to int
        int sum = (dst[cur] & 0xff) * 10 + (Character.digit(ch, 10) & 0xff);
        if (sum > 255) {
          return null;
        }
        dst[cur] = (byte) (sum & 0xff);
        if (!saw_digit) {
          if (++octets > IPv4INADDRSZ) {
            return null;
          }
          saw_digit = true;
        }
      }
      else if (ch == '.' && saw_digit) {
        if (octets == IPv4INADDRSZ) {
          return null;
        }
        cur++;
        dst[cur] = 0;
        saw_digit = false;
      }
      else {
        return null;
      }
    }
    if (octets < IPv4INADDRSZ) {
      return null;
    }
    return dst;
  }

  /**
   * Parses an IPv6 address from the String double dotted representation.
   * 
   * @param src
   *          The String dotted representation of an IPv6Address.
   * @return The ip address as a byte array of size 16 or 4 (for ipv4), or null
   *         if the given String is an invalid ip address.
   */
  public static byte[] parseIPv6Address(String src) {
    // Shortest valid string is "::", hence at least 2 chars
    if (src == null || src.length() < 2) {
      return null;
    }
    int colonp;
    char ch;
    boolean saw_xdigit;
    int val;
    char[] srcb = src.toCharArray();
    byte[] dst = new byte[IPv6INADDRSZ];
    int srcb_length = srcb.length;
    int pc = src.indexOf("%");
    if (pc == srcb_length - 1) {
      return null;
    }
    if (pc != -1) {
      srcb_length = pc;
    }
    colonp = -1;
    int i = 0, j = 0;
    /* Leading :: requires some special handling. */
    if (srcb[i] == ':')
      if (srcb[++i] != ':')
        return null;
    int curtok = i;
    saw_xdigit = false;
    val = 0;
    while (i < srcb_length) {
      ch = srcb[i++];
      int chval = Character.digit(ch, 16);
      if (chval != -1) {
        val <<= 4;
        val |= chval;
        if (val > 0xffff)
          return null;
        saw_xdigit = true;
        continue;
      }
      if (ch == ':') {
        curtok = i;
        if (!saw_xdigit) {
          if (colonp != -1)
            return null;
          colonp = j;
          continue;
        }
        else if (i == srcb_length) {
          return null;
        }
        if (j + INT16SZ > IPv6INADDRSZ)
          return null;
        dst[j++] = (byte) ((val >> 8) & 0xff);
        dst[j++] = (byte) (val & 0xff);
        saw_xdigit = false;
        val = 0;
        continue;
      }
      if (ch == '.' && ((j + IPv4INADDRSZ) <= IPv6INADDRSZ)) {
        String ia4 = src.substring(curtok, srcb_length);
        /* check this IPv4 address has 3 dots, ie. A.B.C.D */
        int dot_count = 0, index = 0;
        while ((index = ia4.indexOf('.', index)) != -1) {
          dot_count++;
          index++;
        }
        if (dot_count != 3) {
          return null;
        }
        byte[] v4addr = parseIPv4Address(ia4);
        if (v4addr == null) {
          return null;
        }
        for (int k = 0; k < IPv4INADDRSZ; k++) {
          dst[j++] = v4addr[k];
        }
        saw_xdigit = false;
        break; /* '\0' was seen by inet_pton4(). */
      }
      return null;
    }
    if (saw_xdigit) {
      if (j + INT16SZ > IPv6INADDRSZ)
        return null;
      dst[j++] = (byte) ((val >> 8) & 0xff);
      dst[j++] = (byte) (val & 0xff);
    }
    if (colonp != -1) {
      int n = j - colonp;
      if (j == IPv6INADDRSZ)
        return null;
      for (i = 1; i <= n; i++) {
        dst[IPv6INADDRSZ - i] = dst[colonp + n - i];
        dst[colonp + n - i] = 0;
      }
      j = IPv6INADDRSZ;
    }
    if (j != IPv6INADDRSZ)
      return null;
    byte[] newdst = convertFromIPv4MappedAddress(dst);
    if (newdst != null) {
      return newdst;
    }
    else {
      return dst;
    }
  }

  /**
   * Converts IPv4 binary address into a string suitable for presentation.
   * 
   * @param src
   *          a byte array representing an IPv4 numeric address
   * @return a String representing the IPv4 address in textual representation
   *         format.
   */
  private static StringBuilder toStringIPv4Address(byte[] src) {
    StringBuilder dottedAddress = new StringBuilder(15);
    dottedAddress.append(src[0] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[1] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[2] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[3] & 0xFF);
    return dottedAddress;
  }

  /**
   * Converts IPv6 binary address into presentation (printable) format.
   * 
   * @param src
   *          a byte array representing the IPv6 numeric address
   * @return a String representing an IPv6 address in textual representation
   *         format
   */
  private static StringBuilder toStringIPv6Address(byte[] src) {
    StringBuilder sb = new StringBuilder(39);
    for (int i = 0; i < (IPv6INADDRSZ / INT16SZ); i++) {
      sb.append(Integer.toHexString(((src[i << 1] << 8) & 0xff00) | (src[(i << 1) + 1] & 0xff)));
      if (i < (IPv6INADDRSZ / INT16SZ) - 1) {
        sb.append(':');
      }
    }
    return sb;
  }

  /**
   * Utility routine to check if the InetAddress is an IPv4 mapped IPv6 address.
   * 
   * @return a <code>boolean</code> indicating if the InetAddress is an IPv4
   *         mapped IPv6 address; or false if address is IPv4 address.
   */
  private static boolean isIPv4MappedAddress(byte[] addr) {
    if (addr.length < IPv6INADDRSZ) {
      return false;
    }
    if ((addr[0] == 0x00) && (addr[1] == 0x00) && (addr[2] == 0x00) && (addr[3] == 0x00) && (addr[4] == 0x00)
        && (addr[5] == 0x00) && (addr[6] == 0x00) && (addr[7] == 0x00) && (addr[8] == 0x00) && (addr[9] == 0x00)
        && (addr[10] == (byte) 0xff) && (addr[11] == (byte) 0xff)) {
      return true;
    }
    return false;
  }

  private static byte[] convertFromIPv4MappedAddress(byte[] addr) {
    if (isIPv4MappedAddress(addr)) {
      byte[] newAddr = new byte[IPv4INADDRSZ];
      System.arraycopy(addr, 12, newAddr, 0, IPv4INADDRSZ);
      return newAddr;
    }
    return null;
  }
}
