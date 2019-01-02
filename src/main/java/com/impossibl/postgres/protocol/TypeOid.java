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
package com.impossibl.postgres.protocol;

import java.util.concurrent.atomic.AtomicReferenceArray;


public class TypeOid implements TypeRef {

  public static final TypeOid INVALID = new TypeOid(0);

  private static final AtomicReferenceArray<TypeOid> fastCachedOids = new AtomicReferenceArray<>(4096);

  public static TypeOid valueOf(int oid) {
    // Craziness is to reduce garbage creation for frequently used types
    if (oid < fastCachedOids.length()) {
      if (fastCachedOids.compareAndSet(oid, null, INVALID)) {
        // Was null, is now INVALID, set to correct value
        TypeOid toid = new TypeOid(oid);
        fastCachedOids.set(oid, toid);
        return toid;
      }
      TypeOid toid = fastCachedOids.get(oid);
      if (toid == INVALID) {
        // Another thread is current creating it... just create garbage...
        toid = new TypeOid(oid);
      }
      return toid;
    }
    return new TypeOid(oid);
  }

  private int oid;

  private TypeOid(int oid) {
    this.oid = oid;
  }

  @Override
  public int getOid() {
    return oid;
  }

  @Override
  public String toString() {
    return "->" + oid;
  }

}
