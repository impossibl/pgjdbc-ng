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

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE_DEFAULT;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;


class ServerConnection implements com.impossibl.postgres.protocol.ServerConnection, RequestExecutor {

  private Channel channel;
  private ServerConnectionShared.Ref sharedRef;

  ServerConnection(Context context, Channel channel, ServerConnectionShared.Ref sharedRef) {
    this.channel = channel;
    this.sharedRef = sharedRef;

    if (context.getSetting(PROTOCOL_TRACE, PROTOCOL_TRACE_DEFAULT)) {

      getMessageDispatchHandler().setTraceWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
    }
  }

  private MessageDispatchHandler getMessageDispatchHandler() {
    return (MessageDispatchHandler) channel.pipeline().context(MessageDispatchHandler.class).handler();
  }

  @Override
  public ChannelFuture shutdown() {

    if (!channel.isActive()) {
      return channel.pipeline().newSucceededFuture();
    }

    // Stop reading while we are shutting down...
    channel.config().setOption(ChannelOption.AUTO_READ, false);

    try {
      ChannelPromise promise = channel.newPromise();
      new ProtocolChannel(channel, StandardCharsets.UTF_8)
          .writeTerminate()
          .addListener(terminated -> {
            // Now kill & wait...
            kill().addListener(killed -> {
              if (killed.cause() != null) {
                promise.setFailure(killed.cause());
              }
              else {
                promise.setSuccess();
              }
            });
          });
      return promise;
    }
    catch (Exception ignore) {
    }

    return kill();
  }

  @Override
  public ChannelFuture kill() {

    if (sharedRef != null) {
      sharedRef.release();
      sharedRef = null;
    }

    return channel.close();
  }

  @Override
  public ByteBufAllocator getAllocator() {
    return channel.alloc();
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return channel.remoteAddress();
  }

  @Override
  public ScheduledExecutorService getIOExecutor() {
    return channel.eventLoop();
  }

  @Override
  public TransactionStatus getTransactionStatus() throws IOException {
    if (!channel.isActive()) {
      throw new ClosedChannelException();
    }
    return getMessageDispatchHandler().getTransactionStatus();
  }

  @Override
  public boolean isConnected() {
    return channel.isActive();
  }

  @Override
  public RequestExecutor getRequestExecutor() {
    return this;
  }

  @Override
  public void query(String sql, QueryHandler handler) {
    submit(new QueryRequest(sql, handler));
  }

  @Override
  public void query(String sql, String portalName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats, int maxRows, ExtendedQueryHandler handler) {
    submit(new ExecuteQueryRequest(sql, portalName, parameterFormats, parameterBuffers, resultFieldFormats, maxRows, handler));
  }

  @Override
  public void prepare(String statementName, String sqlText, Type[] parameterTypes, RequestExecutor.PrepareHandler handler) {
    submit(new PrepareRequest(statementName, sqlText, parameterTypes, handler));
  }

  @Override
  public void execute(String portalName, String statementName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormatRefs, int maxRows, ExecuteHandler handler) {
    submit(new ExecuteStatementRequest(statementName, portalName, parameterFormats, parameterBuffers, resultFieldFormatRefs, maxRows, handler));
  }

  @Override
  public void resume(String portalName, int maxRows, ResumeHandler handler) {
    submit(new ResumePortalRequest(portalName, maxRows, handler));
  }

  @Override
  public void finish(String portalName, SynchronizedHandler handler) {
    submit(new CloseRequest(ServerObjectType.Portal, portalName, handler));
  }

  @Override
  public void lazyExecute(String statementName) {
    submit(new LazyExecuteRequest(statementName));
  }

  @Override
  public void call(int functionId, FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers, RequestExecutor.FunctionCallHandler handler) {
    submit(new FunctionCallRequest(functionId, parameterFormatRefs, parameterBuffers, handler));
  }

  @Override
  public void close(ServerObjectType objectType, String objectName) {
    submit(new CloseRequest(objectType, objectName, null));
  }

  synchronized void submit(ServerRequest request) {

    channel.writeAndFlush(request, channel.voidPromise());
  }

}
