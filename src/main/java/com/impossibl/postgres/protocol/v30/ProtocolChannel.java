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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.FieldFormat.Text;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncode;
import static com.impossibl.postgres.utils.ByteBufs.writeCString;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;

public class ProtocolChannel {

  // Frontend messages
  private static final byte PASSWORD_MSG_ID = 'p';
  private static final byte FLUSH_MSG_ID = 'H';
  private static final byte TERMINATE_MSG_ID = 'X';
  private static final byte SYNC_MSG_ID = 'S';
  private static final byte QUERY_MSG_ID = 'Q';
  private static final byte PARSE_MSG_ID = 'P';
  private static final byte BIND_MSG_ID = 'B';
  private static final byte DESCRIBE_MSG_ID = 'D';
  private static final byte EXECUTE_MSG_ID = 'E';
  private static final byte CLOSE_MSG_ID = 'C';
  private static final byte FUNCTION_CALL_MSG_ID = 'F';

  private Channel channel;
  private ChannelOutboundInvoker flusher;
  private ByteBufAllocator alloc;
  private Charset charset;

  public ProtocolChannel(Channel channel, Charset charset) {
    this(channel, channel, charset);
  }

  public ProtocolChannel(Channel channel, ChannelOutboundInvoker flusher, Charset charset) {
    this.channel = channel;
    this.flusher = flusher;
    this.alloc = channel.alloc();
    this.charset = charset;
  }

  ChannelPipeline pipeline() {
    return channel.pipeline();
  }

  ProtocolChannel flush() {
    flusher.flush();
    return this;
  }

  ProtocolChannel writeSSLRequest() {

    ByteBuf msg = alloc.buffer();

    msg.writeInt(8);
    msg.writeInt(80877103);

    channel.write(msg, channel.voidPromise());

    return this;
  }

  ProtocolChannel writeStartup(Map<String, Object> params) {

    ByteBuf msg = beginMessage((byte) 0);

    // Version
    msg.writeShort(3);
    msg.writeShort(0);

    // Name=Value pairs
    for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
      writeCString(msg, paramEntry.getKey(), charset);
      writeCString(msg, paramEntry.getValue().toString(), charset);
    }

    msg.writeByte(0);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writePassword(String password) {

    ByteBuf msg = beginMessage(PASSWORD_MSG_ID);

    writeCString(msg, password, charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writePassword(ByteBuf password) {

    ByteBuf msg = beginMessage(PASSWORD_MSG_ID);

    msg.writeBytes(password);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeSCM(byte code) {

    ByteBuf msg = alloc.buffer(1);
    msg.writeByte(code);
    channel.write(msg);

    return this;
  }

  ProtocolChannel writeQuery(String query) {

    ByteBuf msg = beginMessage(QUERY_MSG_ID);

    writeCString(msg, query, charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeParse(String stmtName, String query, Type[] paramTypes) {

    ByteBuf msg = beginMessage(PARSE_MSG_ID);

    writeCString(msg, stmtName != null ? stmtName : "", charset);
    writeCString(msg, query, charset);

    msg.writeShort(paramTypes.length);
    for (Type paramType : paramTypes) {
      int paramTypeOid = paramType != null ? paramType.getId() : 0;
      msg.writeInt(paramTypeOid);
    }

    endMessage(msg);

    return this;
  }

  private boolean isAllText(FieldFormatRef[] fieldFormats) {
    return fieldFormats.length == 1 && fieldFormats[0].getFormat() == Text;
  }

  ProtocolChannel writeBind(String portalName, String stmtName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormatRefs) throws IOException {

    byte[] portalNameBytes = nullToEmpty(portalName).getBytes(charset);
    byte[] stmtNameBytes = nullToEmpty(stmtName).getBytes(charset);

    ByteBuf msg = beginMessage(BIND_MSG_ID);

    writeCString(msg, portalNameBytes);
    writeCString(msg, stmtNameBytes);

    loadParams(msg, parameterFormats, parameterBuffers);

    //Set format for results fields
    if (resultFieldFormatRefs == null || resultFieldFormatRefs.length == 0) {
      //Request all binary
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else if (isAllText(resultFieldFormatRefs)) {
      //Shortcut to all text
      msg.writeShort(0);
    }
    else if (!isAllText(resultFieldFormatRefs)) {
      //Select result format for each
      msg.writeShort(resultFieldFormatRefs.length);
      for (FieldFormatRef formatRef : resultFieldFormatRefs) {
        msg.writeShort(formatRef.getFormat().ordinal());
      }
    }

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeDescribe(ServerObjectType target, String targetName) {

    ByteBuf msg = beginMessage(DESCRIBE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeExecute(String portalName, int maxRows) {

    ByteBuf msg = beginMessage(EXECUTE_MSG_ID);

    writeCString(msg, portalName != null ? portalName : "", charset);
    msg.writeInt(maxRows);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeFunctionCall(int functionId, FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers) throws IOException {

    ByteBuf msg = beginMessage(FUNCTION_CALL_MSG_ID);

    msg.writeInt(functionId);

    loadParams(msg, parameterFormatRefs, parameterBuffers);

    msg.writeShort(1);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeClose(ServerObjectType target, String targetName) {

    ByteBuf msg = beginMessage(CLOSE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeFlush() {

    writeMessage(FLUSH_MSG_ID);

    return this;
  }

  ProtocolChannel writeSync() {

    writeMessage(SYNC_MSG_ID);

    return this;
  }

  ChannelFuture writeTerminate() {

    ByteBuf msg = beginMessage(TERMINATE_MSG_ID);

    return channel.writeAndFlush(msg);
  }

  private void writeMessage(byte msgId) {

    ByteBuf msg = alloc.buffer(5);

    msg.writeByte(msgId);
    msg.writeInt(4);

    channel.write(msg, channel.voidPromise());
  }

  private ByteBuf beginMessage(byte msgId) {

    ByteBuf msg = alloc.buffer();

    if (msgId != 0)
      msg.writeByte(msgId);

    msg.markWriterIndex();

    msg.writeInt(-1);

    return msg;
  }

  private void endMessage(ByteBuf msg) {

    int endPos = msg.writerIndex();

    msg.resetWriterIndex();

    int begPos = msg.writerIndex();

    msg.setInt(begPos, endPos - begPos);

    msg.writerIndex(endPos);

    channel.write(msg, channel.voidPromise());
  }

  private void loadParams(ByteBuf msg, FieldFormatRef[] fieldFormatRefs, ByteBuf[] paramBuffers) throws IOException {

    // Select format for parameters
    if (fieldFormatRefs == null) {
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else {
      msg.writeShort(fieldFormatRefs.length);
      for (FieldFormatRef paramFormatRef : fieldFormatRefs) {
        paramFormatRef = paramFormatRef != null ? paramFormatRef : FieldFormat.Text;
        msg.writeShort(paramFormatRef.getFormat().ordinal());
      }
    }

    // Values for each parameter
    if (paramBuffers == null) {
      msg.writeShort(0);
    }
    else {
      msg.writeShort(paramBuffers.length);
      for (ByteBuf paramBuffer : paramBuffers) {
        lengthEncode(msg, paramBuffer, () -> {
          msg.writeBytes(paramBuffer);
          paramBuffer.resetReaderIndex();
        });
      }
    }

  }

}
