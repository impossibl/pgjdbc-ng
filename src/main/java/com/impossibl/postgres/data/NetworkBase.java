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

import org.omg.CORBA.BooleanHolder;

/**
 * 
 * @author http://grepcode.com/snapshot/repo1.maven.org/maven2/org.ancoron.postgresql/org.postgresql.net/9.1.901.jdbc4.1-rc9/
 *
 */
public abstract class NetworkBase {
  protected byte[] addr;
  protected short netmask;
  protected boolean embeddedIpv4 = false;

  public static final int IPv4INADDRSZ = 4;
  public static final int IPv6INADDRSZ = 16;

  NetworkBase(String s) {
    setValue(s);
  }

  NetworkBase(byte[] bytes) {
    this(bytes, (short) (bytes.length == IPv4INADDRSZ ? 32 : 128));
  }

  NetworkBase(byte[] bytes, short mask) {
    setAddress(bytes);
    netmask = mask;
  }

  /**
   * This method sets the value of this inet object.
   * 
   * @param v
   *          A string representation of an inet address a.b.c.d[/netmask]
   * @exception IllegalArgumentException
   *              If the parameter is not a valid inet address.
   */
  public void setValue(String v) {
    if (v == null) {
      throw new IllegalArgumentException("Invalid inet address: " + v);
    }
    int maskIdx = v.indexOf('/');
    String ipaddr;
    short mask = -1;
    if (maskIdx > 0) {
      mask = Short.parseShort(v.substring(maskIdx + 1));
      ipaddr = v.substring(0, maskIdx);
    }
    else {
      ipaddr = v;
    }

    if (isIPv6Address(ipaddr)) {
      setNetmask(mask, 128);
    }
    else if (isIPv4Address(ipaddr, true)) { // allow shortened ipv4 notation
      setNetmask(mask, 32);
    }
    else {
      throw new IllegalArgumentException("Invalid inet: " + v);
    }
  }

  public static byte[] parseIPv4Address(String ipv4Addr, boolean allowShortNotation) {
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
    if (octets < IPv4INADDRSZ && !allowShortNotation) {
      return null;
    }
    return dst;
  }

  /**
   * This will read an IPv4 address string in to the instance variable addr.
   * 
   * <p>
   * If there is a syntax error in the format of the host string false will be
   * returned.
   * 
   * @param host
   *          A host parameter in IPv4 dotted quad notation.
   * @param allowShortNotation
   *          Whether we should accept addresses in the [a[.b[.c[.d]]] format or
   *          a.b.c.d format.
   * @return true if this is an IPv4 host, false if not. If true is returned,
   *         this.addr will contain the valid bytes which make up this IPv4
   *         address.
   */
  protected boolean isIPv4Address(String ipv4Addr, boolean allowShortNotation) {
    this.addr = parseIPv4Address(ipv4Addr, allowShortNotation);
    return addr != null;
  }

  protected void setNetmask(short mask, int max) {
    if (mask == -1) {
      this.netmask = getMinimalNetmask();
    }
    else {
      this.netmask = mask;
      if ((this.netmask < 0) || (this.netmask > max)) {
        this.netmask = 0;
        this.addr = null;
        throw new IllegalArgumentException("Invalid cidr netmask: " + netmask);
      }
    }
  }

  /**
   * This method will get the netmask for a given address.
   * 
   * @param v
   *          is the full address passed to setValue we will extract the netmask
   *          if it exists, if not, we will return a good guess.
   */
  private short getMinimalNetmask() {
    short mask = (short) (this.addr.length * 8);
    if (mask == 32) { // IPv4, IPv6 addresses use 128 if none is specified.
      // If no netmask is specified, we use the smallest netmask that
      // will include all of the non-zero bits of the address in 8 bit
      // blocks.
      int a = this.addr[0] & 0xFF; // cast byte to int and force range 0 -
                                   // 255.
      int b = this.addr[1] & 0xFF;
      int c = this.addr[2] & 0xFF;
      int d = this.addr[3] & 0xFF;
      if ((a >= 0) && (a <= 127)) {
        if (b == 0 && c == 0 && d == 0) {
          mask = 8;
        }
        else if (c == 0 && d == 0) {
          mask = 16;
        }
        else if (d == 0) {
          mask = 24;
        }
      }
      else if ((a >= 128) && (a <= 191)) {
        if (c == 0 && d == 0) {
          mask = 16;
        }
        else if (d == 0) {
          mask = 24;
        }
      }
      else if ((a >= 192) && (a <= 223)) {
        if (d == 0) {
          mask = 24;
        }
      }
    }
    return mask;
  }

  protected static boolean isIPv4MappedAddress(byte[] addr) {
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

  protected void setAddress(byte[] bytes) {
    if ((bytes == null) || ((bytes.length != IPv4INADDRSZ) && (bytes.length != IPv6INADDRSZ))) {
      throw new IllegalArgumentException("Invalid byte array for Inet "
          + (bytes == null ? "null" : bytes.length + ""));
    }
    // should I duplicate the array?
    addr = bytes;
    embeddedIpv4 = isIPv4MappedAddress(addr);
  }

  public static byte[] parseIPv6Address(String src, BooleanHolder embeddedIpV4) {
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
    if (srcb[i] == ':') {
      if (srcb[++i] != ':') {
        return null;
      }
    }
    int curtok = i;
    saw_xdigit = false;
    val = 0;
    embeddedIpV4.value = false;
    while (i < srcb_length) {
      ch = srcb[i++];
      int chval = Character.digit(ch, 16);
      if (chval != -1) {
        val <<= 4;
        val |= chval;
        if (val > 0xffff) {
          return null;
        }
        saw_xdigit = true;
        continue;
      }
      if (ch == ':') {
        curtok = i;
        if (!saw_xdigit) {
          if (colonp != -1) {
            return null;
          }
          colonp = j;
          continue;
        }
        else if (i == srcb_length) {
          return null;
        }
        if (j + 2 > IPv6INADDRSZ) {
          return null;
        }
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
        byte[] v4addr = parseIPv4Address(ia4, false);
        if (v4addr == null) {
          return null;
        }
        embeddedIpV4.value = true;
        for (int k = 0; k < IPv4INADDRSZ; k++) {
          dst[j++] = v4addr[k];
        }
        saw_xdigit = false;
        break; /* '\0' was seen by inet_pton4(). */
      }
      return null;
    }
    if (saw_xdigit) {
      if (j + 2 > IPv6INADDRSZ) {
        return null;
      }
      dst[j++] = (byte) ((val >> 8) & 0xff);
      dst[j++] = (byte) (val & 0xff);
    }
    if (colonp != -1) {
      int n = j - colonp;
      if (j == IPv6INADDRSZ) {
        return null;
      }
      for (i = 1; i <= n; i++) {
        dst[IPv6INADDRSZ - i] = dst[colonp + n - i];
        dst[colonp + n - i] = 0;
      }
      j = IPv6INADDRSZ;
    }
    if (j != IPv6INADDRSZ) {
      return null;
    }
    return dst;
  }

  /**
   * This method will return true if this is a valid syntactically correct IPv6
   * address. It will also fill in the byte array member variable with the 16
   * byte values which make up the address.
   * 
   * @param ipv6Addr
   *          The host portion of the address with the netmask stripped off.
   * @return true if this is a valid IPv6 address, false if not.
   */
  protected boolean isIPv6Address(String ipv6Addr) {
    BooleanHolder embeddedIpV4 = new BooleanHolder(false);
    this.addr = parseIPv6Address(ipv6Addr, embeddedIpV4);
    if (addr != null) {
      this.embeddedIpv4 = embeddedIpV4.value;
      return true;
    }
    return false;
  }

  /**
   * Converts IPv4 binary address into a string suitable for presentation.
   * 
   * @param src
   *          a byte array representing an IPv4 numeric address
   * @return a String representing the IPv4 address in textual representation
   *         format.
   */
  private static void toStringIPv4Address(byte[] src, int offset, StringBuilder dottedAddress) {
    dottedAddress.append(src[offset] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[offset + 1] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[offset + 2] & 0xFF);
    dottedAddress.append('.');
    dottedAddress.append(src[offset + 3] & 0xFF);
  }

  /**
   * Converts IPv6 binary address into presentation (printable) format.
   * 
   * @param src
   *          a byte array representing the IPv6 numeric address
   * @return a String representing an IPv6 address in textual representation
   *         format
   */
  private static void toStringIPv6Address(byte[] src, boolean embeddedIpV4, StringBuilder sb) {
    final int size = embeddedIpV4 ? (IPv6INADDRSZ - IPv4INADDRSZ) / 2 : IPv6INADDRSZ / 2;
    for (int i = 0; i < size; i++) {
      sb.append(Integer.toHexString(((src[i << 1] << 8) & 0xff00) | (src[(i << 1) + 1] & 0xff)));
      if (i < size - 1) {
        sb.append(':');
      }
    }
    if (embeddedIpV4) {
      sb.append(':');
      toStringIPv4Address(src, 12, sb);
    }
  }

  /**
   * Returns the inet address in literal format.
   * 
   * @return A string value of the inet address in literal format.
   */
  @Override
  public String toString() {
    if (this.addr != null) {
      boolean isIPv4 = this.addr.length == IPv4INADDRSZ;
      StringBuilder s = new StringBuilder(isIPv4 ? 18 : 42);
      if (isIPv4) {
        toStringIPv4Address(addr, 0, s);
      }
      else {
        toStringIPv6Address(addr, embeddedIpv4, s);
      }
      if (((!isIPv4) && (this.netmask < 128)) || (isIPv4 && (this.netmask < 32))) {
        s.append('/').append(this.netmask);
      }
      return s.toString();
    }
    return "";
  }

  /**
   * This will return the netmask of the current network address.
   * 
   * @return The netmask of the inet object.
   */
  public short getNetmask() {
    return this.netmask;
  }

  public byte[] getAddress() {
    return addr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(addr);
    result = prime * result + netmask;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof NetworkBase))
      return false;
    NetworkBase other = (NetworkBase) obj;
    if (!Arrays.equals(addr, other.addr))
      return false;
    if (netmask != other.netmask)
      return false;
    return true;
  }
}
