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

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class StreamingByteBuf extends AbstractReferenceCountedByteBuf {

  private final ByteBufAllocator allocator;
  private final ByteOrder endianness;
  private ByteBuf buffer;
  private Channel streamChannel;
  private ChannelFuture lastStreamChannelWrite;
  private int totalWritten;

  public StreamingByteBuf(Channel streamChannel, int estimatedLength) {
    this(streamChannel, ByteOrder.BIG_ENDIAN, estimatedLength);
  }

  public StreamingByteBuf(Channel streamChannel, ByteOrder endianness, int estimatedLength) {
    this(streamChannel, endianness, estimatedLength, streamChannel.alloc());
  }

  public StreamingByteBuf(Channel streamChannel, ByteOrder endianness, int estimatedLength, ByteBufAllocator allocator) {
    super(Integer.MAX_VALUE);
    if (estimatedLength < 0) {
      throw new IllegalArgumentException("estimatedLength: " + estimatedLength);
    }
    if (endianness == null) {
      throw new NullPointerException("endianness");
    }
    if (allocator == null) {
      throw new NullPointerException("allocator");
    }
    this.allocator = allocator;
    this.endianness = endianness;
    this.streamChannel = streamChannel;
    buffer = allocator.buffer(estimatedLength).order(endianness);
  }

  public void flush() {
    ensureWritable(buffer.capacity());
  }

  @Override
  public ByteBuf resetReaderIndex() {
    throw new InvalidMarkException();
  }

  @Override
  public ByteBuf resetWriterIndex() {
    throw new InvalidMarkException();
  }

  @Override
  public ByteBuf unwrap() {
    return buffer;
  }

  public int getTotalLength() {
    return totalWritten + readableBytes();
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {

    if (minWritableBytes > buffer.capacity()) {
      throw new IllegalArgumentException("illegal write size");
    }

    if (minWritableBytes <= writableBytes()) {
      return this;
    }

    if (lastStreamChannelWrite != null)
      lastStreamChannelWrite.syncUninterruptibly();

    totalWritten += readableBytes();

    ByteBuf replacement = alloc().buffer(buffer.capacity()).order(order());

    buffer.setIndex(readerIndex(), writerIndex());

    lastStreamChannelWrite = streamChannel.writeAndFlush(buffer);

    buffer = replacement;

    clear();
    return this;
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
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
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    buffer.getBytes(index, dst);
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return buffer.getBytes(index, out, length);
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    buffer.getBytes(index, out, length);
    return this;
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    buffer.setByte(index, value);
    return this;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    buffer.setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    buffer.setMedium(index, value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    buffer.setInt(index, value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    buffer.setLong(index, value);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    buffer.setBytes(index, src);
    return this;
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
  public ByteBuf writeByte(int value) {
    ensureWritable(1);
    super.writeByte(value);
    return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
    ensureWritable(2);
    super.writeShort(value);
    return this;
  }

  @Override
  public ByteBuf writeMedium(int value) {
    ensureWritable(3);
    super.writeMedium(value);
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    ensureWritable(4);
    super.writeInt(value);
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    ensureWritable(8);
    super.writeLong(value);
    return this;
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    ensureWritable(length);
    super.writeBytes(src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    ensureWritable(length);
    super.writeBytes(src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    ensureWritable(src.remaining());
    super.writeBytes(src);
    return this;
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    ensureWritable(length);
    return super.writeBytes(in, length);
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    ensureWritable(length);
    return super.writeBytes(in, length);
  }

  @Override
  public ByteBuf writeZero(int length) {
    ensureWritable(length);
    super.writeZero(length);
    return this;
  }

  @Override
  public ByteBuf duplicate() {
    return super.duplicate();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return null;
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return null;
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return null;
  }

  @Override
  protected byte _getByte(int index) {
    return buffer.getByte(index);
  }

  @Override
  protected short _getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return buffer.getUnsignedMedium(index);
  }

  @Override
  protected int _getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  protected long _getLong(int index) {
    return buffer.getLong(index);
  }

  @Override
  protected void _setByte(int index, int value) {
    buffer.setByte(index, value);
  }

  @Override
  protected void _setShort(int index, int value) {
    buffer.setShort(index, value);
  }

  @Override
  protected void _setMedium(int index, int value) {
    buffer.setMedium(index, value);
  }

  @Override
  protected void _setInt(int index, int value) {
    buffer.setInt(index, value);
  }

  @Override
  protected void _setLong(int index, long value) {
    buffer.setLong(index, value);
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nioBufferCount() {
    return 0;
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deallocate() {
    buffer.release();
  }
}
