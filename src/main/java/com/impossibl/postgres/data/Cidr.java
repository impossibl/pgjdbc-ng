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

/**
 * 
 * @author http://grepcode.com/file/repo1.maven.org/maven2/org.ancoron.postgresql/org.postgresql.net/9.1.901.jdbc4.1-rc9/org/postgresql/net/PGcidr.java?av=f
 *
 */
public class Cidr extends NetworkBase {

  /**
   * This method will accept an IPv4 or IPv6 network address in any of the
   * following formats.
   * 
   * <p>
   * <ul>
   * <li>A full IPv6 address [RFC 2373] or [RFC 2732] optionally followed by a
   * netmask in the range of 0 to 128.</li>
   * <li>A partial or full IPv4 address, optionally followed by a netmask.
   * a[.b[.c[.d]]][/netmask].</li>
   * </ul>
   * </p>
   * 
   * @param s
   *          The representation of the cidr as a string.
   * @exception IllegalArgumentException
   *              if the string is not in the proper format.
   */
  public Cidr(String s) {
    super(s);
  }

  public Cidr(byte[] bytes) {
    super(bytes);
  }

  public Cidr(byte[] bytes, short mask) {
    super(bytes, mask);
  }

  /**
   * Set the value of this CIDR.
   * 
   * <p>
   * For IPv4 network addresses, this method will accept strings in the
   * following format a[.b[.c[.d]]][/netmask] where a, b, c, d are integers in
   * the range of 0 to 255 and netmask can be an integer from 0 to 32.
   * 
   * <p>
   * IPv6 networks must be entered as a complete IP address as defined in [RFC
   * 2373] or [RFC 2732] optionally followed by a netmask which can be in the
   * range of 0 - 128.
   * 
   * <p>
   * For either IPv4 or IPv6, it is illegal for the host portion of the network
   * address to contain anything but zero values.
   * 
   * @param v
   *          The string representation of this network address.
   * @exception IllegalArgumentException
   *              If it is not in a valid cidr format.
   */
  @Override
  public void setValue(String v) {
    super.setValue(v);
    ensureHostBitsAreZero();
  }

  /**
   * A given IP address and netmask specified with a cidr cannot have any bits
   * which specify the host as non-zero within the address.
   * 
   * <p>
   * This method ensures that this constraint is met.
   * </p>
   * 
   * <p>
   * If the constraint is not met, an exception is thrown.
   * </p>
   */
  protected void ensureHostBitsAreZero() {
    // this represents the number of full bytes in the network portion
    // of the address.
    int network_bytes = this.netmask / 8;

    // This represents the number of bits in the network portion
    // of the address within the boundary byte,
    // if the netmask is 24, this is 0
    // if the netmask is 23, this is 7
    // if the netmask is 25, this is 1 ...
    int network_bits = this.netmask % 8;

    // Test the boundary byte to ensure no bits are set in the host
    // portion.
    if (network_bytes < this.addr.length) {
      if ((this.addr[network_bytes] & (0xFF >> network_bits)) != 0) {
        // bits to the right of the network portion of the
        // address, this is not allowed.
        throw new IllegalArgumentException("host bits not all zero in netmask: " + netmask);
      }
    }

    // test each byte after the boundary byte and ensure that they are zero.
    for (int i = network_bytes + 1; i < this.addr.length; ++i) {
      if (this.addr[i] != 0) {
        // bits to the right of the network portion of the
        // address, this is not allowed.

        throw new IllegalArgumentException("host bits not all zero in netmask: " + netmask);
      }
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o) && (o instanceof Cidr);
  }
}
