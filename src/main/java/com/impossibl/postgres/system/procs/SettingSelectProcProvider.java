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

import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type.Codec;

import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public class SettingSelectProcProvider extends BaseProcProvider {

  Function<ServerInfo, Boolean> check;
  Codec.Encoder<StringBuilder> enabledTxtEncoder;
  Codec.Decoder<CharSequence> enabledTxtDecoder;
  Codec.Encoder<ByteBuf> enabledBinEncoder;
  Codec.Decoder<ByteBuf> enabledBinDecoder;
  Codec.Encoder<StringBuilder> disabledTxtEncoder;
  Codec.Decoder<CharSequence> disabledTxtDecoder;
  Codec.Encoder<ByteBuf> disabledBinEncoder;
  Codec.Decoder<ByteBuf> disabledBinDecoder;

  public SettingSelectProcProvider(
      Function<ServerInfo, Boolean> check,
      Codec.Encoder<StringBuilder> enabledTxtEncoder, Codec.Decoder<CharSequence> enabledTxtDecoder,
      Codec.Encoder<ByteBuf> enabledBinEncoder, Codec.Decoder<ByteBuf> enabledBinDecoder,
      Codec.Encoder<StringBuilder> disabledTxtEncoder, Codec.Decoder<CharSequence> disabledTxtDecoder,
      Codec.Encoder<ByteBuf> disabledBinEncoder, Codec.Decoder<ByteBuf> disabledBinDecoder, String... baseNames) {
    super(baseNames);
    this.check = check;
    this.enabledTxtEncoder = enabledTxtEncoder;
    this.enabledTxtDecoder = enabledTxtDecoder;
    this.enabledBinEncoder = enabledBinEncoder;
    this.enabledBinDecoder = enabledBinDecoder;
    this.disabledTxtEncoder = disabledTxtEncoder;
    this.disabledTxtDecoder = disabledTxtDecoder;
    this.disabledBinEncoder = disabledBinEncoder;
    this.disabledBinDecoder = disabledBinDecoder;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <Buffer> Codec.Encoder<Buffer> findEncoder(String name, ServerInfo serverInfo, Class<? extends Buffer> bufferType) {
    if (bufferType == ByteBuf.class && name.endsWith("recv") && hasName(name, "recv", serverInfo)) {
      if (check.apply(serverInfo))
        return (Codec.Encoder<Buffer>) enabledBinEncoder;
      else
        return (Codec.Encoder<Buffer>) disabledBinEncoder;
    }
    else if (bufferType == StringBuilder.class && name.endsWith("in") && hasName(name, "in", serverInfo)) {
      if (check.apply(serverInfo))
        return (Codec.Encoder<Buffer>) enabledTxtEncoder;
      else
        return (Codec.Encoder<Buffer>) disabledTxtEncoder;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <Buffer> Codec.Decoder<Buffer> findDecoder(String name, ServerInfo serverInfo, Class<? extends Buffer> bufferType) {
    if (bufferType == ByteBuf.class && name.endsWith("send") && hasName(name, "send", serverInfo)) {
      if (check.apply(serverInfo))
        return (Codec.Decoder<Buffer>) enabledBinDecoder;
      else
        return (Codec.Decoder<Buffer>) disabledBinDecoder;
    }
    else if (bufferType == CharSequence.class && name.endsWith("out") && hasName(name, "out", serverInfo)) {
      if (check.apply(serverInfo))
        return (Codec.Decoder<Buffer>) enabledTxtDecoder;
      else
        return (Codec.Decoder<Buffer>) disabledTxtDecoder;
    }
    return null;
  }

  @Override
  public Modifiers.Parser findModifierParser(String name, ServerInfo serverInfo) {
    return null;
  }

}
