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

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

public class Domains extends SimpleProcProvider {

  public Domains() {
    super(new TxtEncoder(), null, new BinEncoder(), null, "domain_");
  }

  public static class BinEncoder extends BinaryEncoder {

    @Override
    public Class<?> getInputType() {
      return null;  //any
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return null;  //any
    }

    @Override
    public void encode(Type type, ChannelBuffer buffer, Object value, Context context) throws IOException {

      DomainType domainType = (DomainType) type;
      Type baseType = domainType.getBase();

      baseType.getBinaryCodec().encoder.encode(baseType, buffer, value, context);
    }

  }

  public static class TxtEncoder extends TextEncoder {

    @Override
    public Class<?> getInputType() {
      return null;  //any
    }

    @Override
    public PrimitiveType getOutputPrimitiveType() {
      return null;  //any
    }

    @Override
    public void encode(Type type, StringBuilder buffer, Object value, Context context) throws IOException {

      DomainType domainType = (DomainType) type;
      Type baseType = domainType.getBase();

      baseType.getTextCodec().encoder.encode(baseType, buffer, value, context);
    }

  }

}
