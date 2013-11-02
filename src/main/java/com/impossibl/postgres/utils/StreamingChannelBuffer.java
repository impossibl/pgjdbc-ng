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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.jboss.netty.buffer.AbstractChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.DuplicatedChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.buffer.WrappedChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;



public class StreamingChannelBuffer extends AbstractChannelBuffer implements WrappedChannelBuffer {

  private final ChannelBufferFactory factory;
  private final ByteOrder endianness;
  private ChannelBuffer buffer;
  private Channel streamChannel;
  private ChannelFuture lastStreamChannelWrite;
  private int totalWritten;

  public StreamingChannelBuffer(Channel streamChannel, int estimatedLength) {
    this(streamChannel, ByteOrder.BIG_ENDIAN, estimatedLength);
  }

  public StreamingChannelBuffer(Channel streamChannel, ByteOrder endianness, int estimatedLength) {
    this(streamChannel, endianness, estimatedLength, HeapChannelBufferFactory.getInstance(endianness));
  }

  public StreamingChannelBuffer(Channel streamChannel, ByteOrder endianness, int estimatedLength, ChannelBufferFactory factory) {
    if (estimatedLength < 0) {
      throw new IllegalArgumentException("estimatedLength: " + estimatedLength);
    }
    if (endianness == null) {
      throw new NullPointerException("endianness");
    }
    if (factory == null) {
      throw new NullPointerException("factory");
    }
    this.factory = factory;
    this.endianness = endianness;
    this.streamChannel = streamChannel;
    buffer = factory.getBuffer(order(), estimatedLength);
  }

  public void flush() {
    ensureWritableBytes(buffer.capacity());
  }

  @Override
  public void resetReaderIndex() {
    throw new InvalidMarkException();
  }

  @Override
  public void resetWriterIndex() {
    throw new InvalidMarkException();
  }

  @Override
  public ChannelBuffer unwrap() {
    return buffer;
  }

  public int getTotalLength() {
    return totalWritten + readableBytes();
  }

  @Override
  public void ensureWritableBytes(int minWritableBytes) {

    if (minWritableBytes > buffer.capacity()) {
      throw new IllegalArgumentException("illegal write size");
    }

    if (minWritableBytes <= writableBytes()) {
      return;
    }

    if (lastStreamChannelWrite != null)
      lastStreamChannelWrite.syncUninterruptibly();

    totalWritten += readableBytes();

    ChannelBuffer replacement = factory.getBuffer(order(), buffer.capacity());

    buffer.setIndex(readerIndex(), writerIndex());

    lastStreamChannelWrite = streamChannel.write(buffer);

    buffer = replacement;

    clear();
  }

  @Override
  public ChannelBufferFactory factory() {
    return factory;
  }

  @Override
  public ByteOrder order() {
    return endianness;
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public boolean hasArray() {
    return buffer.hasArray();
  }

  @Override
  public byte[] array() {
    return buffer.array();
  }

  @Override
  public int arrayOffset() {
    return buffer.arrayOffset();
  }

  @Override
  public byte getByte(int index) {
    return buffer.getByte(index);
  }

  @Override
  public short getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return buffer.getUnsignedMedium(index);
  }

  @Override
  public int getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong(int index) {
    return buffer.getLong(index);
  }

  @Override
  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
  }

  @Override
  public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
  }

  @Override
  public void getBytes(int index, ByteBuffer dst) {
    buffer.getBytes(index, dst);
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return buffer.getBytes(index, out, length);
  }

  @Override
  public void getBytes(int index, OutputStream out, int length) throws IOException {
    buffer.getBytes(index, out, length);
  }

  @Override
  public void setByte(int index, int value) {
    buffer.setByte(index, value);
  }

  @Override
  public void setShort(int index, int value) {
    buffer.setShort(index, value);
  }

  @Override
  public void setMedium(int index, int value) {
    buffer.setMedium(index, value);
  }

  @Override
  public void setInt(int index, int value) {
    buffer.setInt(index, value);
  }

  @Override
  public void setLong(int index, long value) {
    buffer.setLong(index, value);
  }

  @Override
  public void setBytes(int index, byte[] src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
  }

  @Override
  public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
  }

  @Override
  public void setBytes(int index, ByteBuffer src) {
    buffer.setBytes(index, src);
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return buffer.setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return buffer.setBytes(index, in, length);
  }

  @Override
  public void writeByte(int value) {
    ensureWritableBytes(1);
    super.writeByte(value);
  }

  @Override
  public void writeShort(int value) {
    ensureWritableBytes(2);
    super.writeShort(value);
  }

  @Override
  public void writeMedium(int value) {
    ensureWritableBytes(3);
    super.writeMedium(value);
  }

  @Override
  public void writeInt(int value) {
    ensureWritableBytes(4);
    super.writeInt(value);
  }

  @Override
  public void writeLong(long value) {
    ensureWritableBytes(8);
    super.writeLong(value);
  }

  @Override
  public void writeBytes(byte[] src, int srcIndex, int length) {
    ensureWritableBytes(length);
    super.writeBytes(src, srcIndex, length);
  }

  @Override
  public void writeBytes(ChannelBuffer src, int srcIndex, int length) {
    ensureWritableBytes(length);
    super.writeBytes(src, srcIndex, length);
  }

  @Override
  public void writeBytes(ByteBuffer src) {
    ensureWritableBytes(src.remaining());
    super.writeBytes(src);
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    ensureWritableBytes(length);
    return super.writeBytes(in, length);
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    ensureWritableBytes(length);
    return super.writeBytes(in, length);
  }

  @Override
  public void writeZero(int length) {
    ensureWritableBytes(length);
    super.writeZero(length);
  }

  @Override
  public ChannelBuffer duplicate() {
    return new DuplicatedChannelBuffer(this);
  }

  @Override
  public ChannelBuffer copy(int index, int length) {
    return null;
  }

  @Override
  public ChannelBuffer slice(int index, int length) {
    return null;
  }

  @Override
  public ByteBuffer toByteBuffer(int index, int length) {
    return null;
  }

}
