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
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type.Codec;



public class SimpleProcProvider extends BaseProcProvider {

  Codec.Encoder txtEncoder;
  Codec.Decoder txtDecoder;
  Codec.Encoder binEncoder;
  Codec.Decoder binDecoder;
  Modifiers.Parser modParser;

  public SimpleProcProvider(Codec.Encoder txtEncoder, Codec.Decoder txtDecoder, Codec.Encoder binEncoder, Codec.Decoder binDecoder, String... baseNames) {
    this(txtEncoder, txtDecoder, binEncoder, binDecoder, null, baseNames);
  }

  public SimpleProcProvider(Codec.Encoder txtEncoder, Codec.Decoder txtDecoder, Codec.Encoder binEncoder, Codec.Decoder binDecoder, Modifiers.Parser modParser, String... baseNames) {
    super(baseNames);
    this.txtEncoder = txtEncoder;
    this.txtDecoder = txtDecoder;
    this.binEncoder = binEncoder;
    this.binDecoder = binDecoder;
    this.modParser = modParser;
  }

  public SimpleProcProvider(Modifiers.Parser modParser, String... baseNames) {
    super(baseNames);
    this.modParser = modParser;
  }

  public Codec.Encoder findEncoder(String name, Context context) {
    if (name.endsWith("recv") && hasName(name, "recv", context)) {
      return binEncoder;
    }
    else if (name.endsWith("in") && hasName(name, "in", context)) {
      return txtEncoder;
    }
    return null;
  }

  public Codec.Decoder findDecoder(String name, Context context) {
    if (name.endsWith("send") && hasName(name, "send", context)) {
      return binDecoder;
    }
    else if (name.endsWith("out") && hasName(name, "out", context)) {
      return txtDecoder;
    }
    return null;
  }

  @Override
  public Modifiers.Parser findModifierParser(String name, Context context) {
    if (name.endsWith("typmodin") && hasName(name, "typmodin", context)) {
      return modParser;
    }
    if (name.endsWith("typmodout") && hasName(name, "typmodout", context)) {
      return modParser;
    }
    return null;
  }

}
