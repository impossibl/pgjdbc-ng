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
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.jboss.netty.buffer.AbstractChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;

public class NullChannelBuffer extends AbstractChannelBuffer {

  @Override
  public ChannelBufferFactory factory() {
    return null;
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
  public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
  }

  @Override
  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
  }

  @Override
  public void getBytes(int index, ByteBuffer dst) {
  }

  @Override
  public void getBytes(int index, OutputStream out, int length) throws IOException {
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return 0;
  }

  @Override
  public void setByte(int index, int value) {
  }

  @Override
  public void setShort(int index, int value) {
  }

  @Override
  public void setMedium(int index, int value) {
  }

  @Override
  public void setInt(int index, int value) {
  }

  @Override
  public void setLong(int index, long value) {
  }

  @Override
  public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
  }

  @Override
  public void setBytes(int index, byte[] src, int srcIndex, int length) {
  }

  @Override
  public void setBytes(int index, ByteBuffer src) {
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
  public ChannelBuffer copy(int index, int length) {
    return null;
  }

  @Override
  public ChannelBuffer slice(int index, int length) {
    return null;
  }

  @Override
  public ChannelBuffer duplicate() {
    return null;
  }

  @Override
  public ByteBuffer toByteBuffer(int index, int length) {
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

}
