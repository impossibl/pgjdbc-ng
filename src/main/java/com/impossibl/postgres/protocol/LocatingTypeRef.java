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

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.TypeRef;


public class LocatingTypeRef implements TypeRef {

  private Object val;

  public Type getType(Context context) {
    if (val instanceof Locator) {
      val = ((Locator) val).locate(context);
    }
    return (Type) val;
  }

  private LocatingTypeRef(Object val) {
    this.val = val;
  }

  public static LocatingTypeRef from(int typeId) {
    return new LocatingTypeRef(new Locator(typeId));
  }

  @Override
  public String toString() {
    if (val instanceof Locator) {
      return "->" + val.toString();
    }
    return val.toString();
  }

  static class Locator {

    private int typeId;

    Locator(int typeId) {
      this.typeId = typeId;
    }

    Type locate(Context context) {
      return context.getRegistry().loadType(typeId);
    }

    @Override
    public String toString() {
      return Integer.toString(typeId);
    }

  }

}
