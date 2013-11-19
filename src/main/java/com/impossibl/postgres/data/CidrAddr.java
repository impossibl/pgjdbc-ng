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



public class CidrAddr extends InetAddr {

  private CidrAddr(Object[] parts) {
    this((byte[]) parts[0], (short) parts[1]);
  }

  public CidrAddr(byte[] address, short maskBits) throws IllegalArgumentException {
    super(address, maskBits);
    checkHostBits(address, maskBits);
  }

  public CidrAddr(String cidrAddress) throws IllegalArgumentException {
    this(parseN(cidrAddress, true));
  }

  @Override
  public void setAddress(byte[] address) {
    this.address = address;
    this.maskBits = (short) (address.length * 8);
  }

  @Override
  public void setMaskBits(short maskBits) {
    checkHostBits(address, maskBits);
    this.maskBits = maskBits;
  }

  public static CidrAddr parseCidrAddr(String cidrAddr) throws IllegalArgumentException {
    return new CidrAddr(parseN(cidrAddr, true));
  }

  static void checkHostBits(byte[] address, short maskBits) throws IllegalArgumentException {

    // this represents the number of full bytes in the network portion of the
    // address.
    int networkBytes = maskBits / 8;

    // This represents the number of bits in the network portion of the address
    // within the boundary byte,
    // if the netmask is 24, this is 0
    // if the netmask is 23, this is 7
    // if the netmask is 25, this is 1 ...
    int networkBits = maskBits % 8;

    // Test the boundary byte to ensure no bits are set in the host portion.
    if (networkBytes < address.length) {

      if ((address[networkBytes] & (0xFF >> networkBits)) != 0) {
        // bits to the right of the network portion of the address, this is not
        // allowed.
        throw new IllegalArgumentException("host bits not all zero in netmask: " + maskBits);
      }
    }

    // Test each byte after the boundary byte and ensure that they are zero.
    for (int i = networkBytes + 1; i < address.length; ++i) {

      if (address[i] != 0) {

        // bits to the right of the network portion of the address, this is not
        // allowed.

        throw new IllegalArgumentException("host bits not all zero in netmask: " + maskBits);
      }
    }

  }

}
