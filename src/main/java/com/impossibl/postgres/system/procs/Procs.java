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

import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

import java.util.ServiceLoader;


public class Procs {

  /*
   * Master ProcProvider List (Loaded as service through META-INF/services/*.ProcProvider)
   */
  /*
  private static final ProcProvider[] PROVIDERS = {
    new ACLItems(),
    new Arrays(),
    new BitMods(),
    new Bits(),
    new Bools(),
    new Boxes(),
    new Bytes(),
    new Cidrs(),
    new Circles(),
    new Dates(),
    new Domains(),
    new Float4s(),
    new Float8s(),
    new HStores(),
    new Inets(),
    new Int2s(),
    new Int4s(),
    new Int8s(),
    new Intervals(),
    new Lines(),
    new LSegs(),
    new MacAddrs(),
    new Moneys(),
    new Names(),
    new NumericMods(),
    new Numerics(),
    new Oids(),
    new Paths(),
    new Points(),
    new Polygons(),
    new Ranges(),
    new Records(),
    new Strings(),
    new Tids(),
    new TimeMods(),
    new TimestampMods(),
    new TimestampsWithoutTZ(),
    new TimestampsWithTZ(),
    new TimesWithoutTZ(),
    new TimesWithTZ(),
    new UUIDs(),
    new XMLs(),
  };
  */

  private static final Type.Codec.Decoder[] DEFAULT_DECODERS = {new Unknowns.TxtDecoder(), new Unknowns.BinDecoder()};
  private static final Type.Codec.Encoder[] DEFAULT_ENCODERS = {new Unknowns.TxtEncoder(), new Unknowns.BinEncoder()};
  private static final Modifiers.Parser DEFAULT_MOD_PARSER = new Unknowns.ModParser();

  private ServiceLoader<ProcProvider> providers;

  public Procs(ClassLoader classLoader) {
    providers = ServiceLoader.load(ProcProvider.class, classLoader);
  }

  public Type.Codec.Decoder getDefaultDecoder(Format format) {
    return DEFAULT_DECODERS[format.ordinal()];
  }

  public Type.Codec.Encoder getDefaultEncoder(Format format) {
    return DEFAULT_ENCODERS[format.ordinal()];
  }

  public Modifiers.Parser getDefaultModParser() {
    return DEFAULT_MOD_PARSER;
  }


  public Codec loadNamedTextCodec(String baseName, Context context) {
    Codec codec = new Codec();
    codec.encoder = loadEncoderProc(baseName + "in",  context, DEFAULT_ENCODERS[Format.Text.ordinal()]);
    codec.decoder = loadDecoderProc(baseName + "out", context, DEFAULT_DECODERS[Format.Text.ordinal()]);
    return codec;
  }

  public Codec loadNamedBinaryCodec(String baseName, Context context) {
    Codec codec = new Codec();
    codec.encoder = loadEncoderProc(baseName + "recv", context, DEFAULT_ENCODERS[Format.Binary.ordinal()]);
    codec.decoder = loadDecoderProc(baseName + "send", context, DEFAULT_DECODERS[Format.Binary.ordinal()]);
    return codec;
  }

  public Codec.Encoder loadEncoderProc(String name, Context context, Type.Codec.Encoder defaultEncoder) {

    if (!name.isEmpty()) {
      Codec.Encoder h;

      for (ProcProvider pp : providers) {
        if ((h = pp.findEncoder(name, context)) != null)
          return h;
      }
    }

    return defaultEncoder;
  }

  public Codec.Decoder loadDecoderProc(String name, Context context, Type.Codec.Decoder defaultDecoder) {
    if (!name.isEmpty()) {
      Codec.Decoder h;

      for (ProcProvider pp : providers) {
        if ((h = pp.findDecoder(name, context)) != null)
          return h;
      }
    }

    return defaultDecoder;
  }

  public Modifiers.Parser loadModifierParserProc(String name, Context context) {

    if (!name.isEmpty()) {
      Modifiers.Parser p;

      for (ProcProvider pp : providers) {
        if ((p = pp.findModifierParser(name, context)) != null)
          return p;
      }
    }

    return DEFAULT_MOD_PARSER;
  }

}
