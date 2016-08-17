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

import com.impossibl.postgres.api.data.Tid;
import com.impossibl.postgres.jdbc.PGRowId;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.sql.RowId;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;


public class Tids extends SimpleProcProvider {

  public Tids() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "tid");
  }

  static Tid convertInput(Context context, Object source, Object sourceContext) {

    if (source instanceof Tid) {
      return (Tid) source;
    }

    if (source instanceof PGRowId) {
      return ((PGRowId) source).getTid();
    }

    if (source instanceof String) {
      parseTid((String) source);
    }

    return null;
  }

  static Object convertOutput(Context context, Tid decoded, Class<?> targetClass, Object targetContext) {

    if (targetClass == Tid.class) {
      return decoded;
    }

    if (targetClass == RowId.class) {
      return new PGRowId(decoded);
    }

    if (targetClass == String.class) {
      return "(" + decoded.getBlock() + "," + decoded.getOffset() + ")";
    }

    return null;
  }

  static class BinDecoder extends AutoConvertingBinaryDecoder<Tid> {

    BinDecoder() {
      super(6, Tids::convertOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<Tid> getDefaultClass() {
      return Tid.class;
    }

    @Override
    protected Tid decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int block = buffer.readInt();
      short offset = buffer.readShort();

      return new Tid(block, offset);
    }

  }

  static class BinEncoder extends AutoConvertingBinaryEncoder<Tid> {

    BinEncoder() {
      super(6, Tids::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    protected Class<Tid> getDefaultClass() {
      return Tid.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Tid value, Object sourceContext, ByteBuf buffer) throws IOException {

      buffer.writeInt(value.getBlock());
      buffer.writeShort(value.getOffset());
    }

  }

  static class TxtDecoder extends AutoConvertingTextDecoder<Tid> {

    TxtDecoder() {
      super(Tids::convertOutput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    public Class<Tid> getDefaultClass() {
      return Tid.class;
    }

    @Override
    protected Tid decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      return parseTid(buffer);
    }

  }

  static class TxtEncoder extends AutoConvertingTextEncoder<Tid> {

    TxtEncoder() {
      super(Tids::convertInput);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return PrimitiveType.Tid;
    }

    @Override
    protected Class<Tid> getDefaultClass() {
      return Tid.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Tid value, Object sourceContext, StringBuilder buffer) throws IOException {

      formatTid(value, buffer);
    }

  }

  private static Tid parseTid(CharSequence source) {

    String[] items = source.subSequence(1, source.length() - 1).toString().split(",");

    int block = Integer.parseInt(items[0]);
    short offset = Short.parseShort(items[1]);

    return new Tid(block, offset);
  }

  private static void formatTid(Tid tid, StringBuilder out) {
    out.append('(').append(tid.getBlock()).append(',').append(tid.getOffset()).append(')');
  }

}
