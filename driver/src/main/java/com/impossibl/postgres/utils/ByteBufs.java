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
package com.impossibl.postgres.utils;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;

public class ByteBufs {

  public static ByteBuf[] allocAll(ByteBufAllocator alloc, int count) {
    ByteBuf[] buffers = new ByteBuf[count];
    for (int c = 0; c < count; ++c)
      buffers[c] = alloc.buffer();
    return buffers;
  }

  public static ByteBuf[] duplicateAll(ByteBuf[] buffers) {
    buffers = buffers.clone();
    for (int c = 0; c < buffers.length; ++c)
      buffers[c] = buffers[c].duplicate();
    return buffers;
  }

  public static ByteBuf[] retainedDuplicateAll(ByteBuf[] buffers) {
    buffers = buffers.clone();
    for (int c = 0; c < buffers.length; ++c)
      buffers[c] = buffers[c].retainedDuplicate();
    return buffers;
  }

  public static void releaseAll(ByteBuf[] byteBufs) {
    for (int c = 0; c < byteBufs.length; ++c) {
      ReferenceCountUtil.release(byteBufs[c]);
      byteBufs[c] = null;
    }
  }

  public interface EncodeFunction {
    void encode() throws IOException;
  }

  public static ByteBuf[] encode(ByteBufAllocator alloc, CharSequence[] textBuffers) {
    ByteBuf[] binaryBuffers = new ByteBuf[textBuffers.length];
    for (int bufferIdx = 0; bufferIdx < textBuffers.length; ++bufferIdx) {
      binaryBuffers[bufferIdx] = ByteBufUtil.writeUtf8(alloc, textBuffers[bufferIdx]);
    }
    return  binaryBuffers;
  }

  public static void lengthEncodeBinary(Type.Codec.Encoder<ByteBuf> encoder, Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {
    lengthEncode(buffer, value, () -> encoder.encode(context, type, value, sourceContext, buffer));
  }

  public static int lengthEncode(ByteBuf buffer, Object value, EncodeFunction encode) throws IOException {

    int lengthOff = buffer.writerIndex();

    buffer.writeInt(-1);
    if (value == null) {
      return lengthOff;
    }

    int dataOff = buffer.writerIndex();

    encode.encode();

    buffer.setInt(lengthOff, buffer.writerIndex() - dataOff);

    return lengthOff;
  }

  public interface DecodeFunction {
    Object decode(ByteBuf buffer) throws IOException;
  }

  public static Object lengthDecodeBinary(Type.Codec.Decoder<ByteBuf> decoder, Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {
    return lengthDecode(buffer, data -> decoder.decode(context, type, typeLength, typeModifier, data, targetClass, targetContext));
  }

  public static Object lengthDecode(ByteBuf buffer, DecodeFunction decode) throws IOException {

    int length = buffer.readInt();
    if (length == -1) {
      return null;
    }

    ByteBuf data = buffer.readRetainedSlice(length);
    try {
      return decode.decode(data);
    }
    finally {
      data.release();
    }
  }

  public static String readCString(ByteBuf buffer, Charset charset) {

    int strLen = buffer.bytesBefore((byte) 0);
    byte[] bytes = new byte[strLen];
    buffer.readBytes(bytes);
    buffer.skipBytes(1);

    return new String(bytes, 0, bytes.length, charset);
  }

  public static void writeCString(ByteBuf buffer, String val, Charset charset) {

    writeCString(buffer, val.getBytes(charset));
  }

  public static void writeCString(ByteBuf buffer, byte[] valBytes) {

    buffer.writeBytes(valBytes);
    buffer.writeByte(0);
  }

}
