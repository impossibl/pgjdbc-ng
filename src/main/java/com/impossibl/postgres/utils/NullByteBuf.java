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
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class NullByteBuf extends AbstractByteBuf {

  public NullByteBuf() {
    super(Integer.MAX_VALUE);
  }

  @Override
  public int capacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public ByteOrder order() {
    return null;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public byte getByte(int index) {
    return 0;
  }

  @Override
  public short getShort(int index) {
    return 0;
  }

  @Override
  public int getUnsignedMedium(int index) {
    return 0;
  }

  @Override
  public int getInt(int index) {
    return 0;
  }

  @Override
  public long getLong(int index) {
    return 0;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return 0;
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    return 0;
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    return 0;
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    return this;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    return this;
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return 0;
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return 0;
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
  public ByteBuf duplicate() {
    return null;
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return null;
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    return null;
  }

  @Override
  public int arrayOffset() {
    return 0;
  }

  @Override
  protected byte _getByte(int index) {
    return 0;
  }

  @Override
  protected short _getShort(int index) {
    return 0;
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return 0;
  }

  @Override
  protected int _getInt(int index) {
    return 0;
  }

  @Override
  protected long _getLong(int index) {
    return 0;
  }

  @Override
  protected short _getShortLE(int index) {
    return 0;
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    return 0;
  }

  @Override
  protected int _getIntLE(int index) {
    return 0;
  }

  @Override
  protected long _getLongLE(int index) {
    return 0;
  }

  @Override
  protected void _setByte(int index, int value) {

  }

  @Override
  protected void _setShort(int index, int value) {

  }

  @Override
  protected void _setMedium(int index, int value) {

  }

  @Override
  protected void _setInt(int index, int value) {

  }

  @Override
  protected void _setLong(int index, long value) {

  }

  @Override
  protected void _setShortLE(int index, int value) {

  }

  @Override
  protected void _setMediumLE(int index, int value) {

  }

  @Override
  protected void _setIntLE(int index, int value) {

  }

  @Override
  protected void _setLongLE(int index, long value) {

  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    return null;
  }

  @Override
  public ByteBufAllocator alloc() {
    return null;
  }

  @Override
  public ByteBuf unwrap() {
    return null;
  }

  @Override
  public int nioBufferCount() {
    return 0;
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return null;
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return new ByteBuffer[0];
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    return 0;
  }

  @Override
  public ByteBuf retain(int increment) {
    return this;
  }

  @Override
  public ByteBuf retain() {
    return this;
  }

  @Override
  public int refCnt() {
    return 1;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }

  @Override
  public ByteBuf touch() {
    return null;
  }

  @Override
  public ByteBuf touch(Object hint) {
    return null;
  }
}
