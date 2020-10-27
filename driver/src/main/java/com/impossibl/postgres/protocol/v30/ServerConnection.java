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
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.system.Configuration;
import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.system.Version;

import static com.impossibl.postgres.system.SystemSettings.SQL_TRACE;
import static com.impossibl.postgres.system.SystemSettings.SQL_TRACE_FILE;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;


class ServerConnection implements com.impossibl.postgres.protocol.ServerConnection, RequestExecutor {

  private Channel channel;
  private ServerInfo serverInfo;
  private Version protocolVersion;
  private KeyData keyData;
  private ServerConnectionShared.Ref sharedRef;
  private SQLTrace sqlTrace;

  ServerConnection(Configuration config, Channel channel, ServerInfo serverInfo, Version protocolVersion, KeyData keyData, ServerConnectionShared.Ref sharedRef) {
    this.channel = channel;
    this.serverInfo = serverInfo;
    this.protocolVersion = protocolVersion;
    this.keyData = keyData;
    this.sharedRef = sharedRef;

    if (config.getSetting(SQL_TRACE)) {
      OutputStream out = System.out;
      String filePath = config.getSetting(SQL_TRACE_FILE);
      if (filePath != null) {
        try {
          out = new FileOutputStream(filePath, false);
        }
        catch (FileNotFoundException ignore) {
        }
      }
      sqlTrace = new SQLTrace(new OutputStreamWriter(out));
    }
  }

  MessageDispatchHandler getMessageDispatchHandler() {
    return (MessageDispatchHandler) channel.pipeline().context(MessageDispatchHandler.class).handler();
  }

  @Override
  public ServerInfo getServerInfo() {
    return serverInfo;
  }

  @Override
  public Version getProtocolVersion() {
    return protocolVersion;
  }

  @Override
  public KeyData getKeyData() {
    return keyData;
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
  public void sync(SynchronizedHandler handler) throws IOException {
    submit(new SynchronizeRequest(handler));
  }

  @Override
  public void query(String sql, QueryHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query(sql);
    }
    submit(new QueryRequest(sql, handler));
  }

  @Override
  public void query(String sql, String portalName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats, int maxRows, ExtendedQueryHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query(sql);
    }
    submit(new ExecuteQueryRequest(sql, portalName, parameterFormats, parameterBuffers, resultFieldFormats, maxRows, handler));
  }

  @Override
  public void query(String portalName, int maxRows, ExtendedQueryHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query(portalName);
    }
    submit(new ExecutePortalRequest(portalName, maxRows, handler));
  }

  @Override
  public void prepare(String statementName, String sql, TypeRef[] parameterTypes, RequestExecutor.PrepareHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.prepare(statementName, sql);
    }
    submit(new PrepareRequest(statementName, sql, parameterTypes, handler));
  }

  @Override
  public void execute(String portalName, String statementName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats, int maxRows, ExecuteHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.execute(statementName);
    }
    submit(new ExecuteStatementRequest(statementName, portalName, parameterFormats, parameterBuffers, resultFieldFormats, maxRows, handler));
  }

  @Override
  public void resume(String portalName, int maxRows, ResumeHandler handler) throws IOException {
    submit(new ResumePortalRequest(portalName, maxRows, handler));
  }

  @Override
  public void finish(String portalName, SynchronizedHandler handler) throws IOException {
    submit(new CloseRequest(ServerObjectType.Portal, portalName, handler));
  }

  @Override
  public void lazyExecute(String statementName) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query(statementName);
    }
    submit(new LazyExecuteRequest(statementName));
  }

  @Override
  public void call(int functionId, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, RequestExecutor.FunctionCallHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query("CALL: " + functionId);
    }
    submit(new FunctionCallRequest(functionId, parameterFormats, parameterBuffers, handler));
  }

  @Override
  public void copyFrom(String sql, InputStream stream, CopyFromHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query("COPY-IN: " + sql);
    }
    submit(new CopyInRequest(sql, stream, handler));
  }

  @Override
  public void copyTo(String sql, OutputStream stream, CopyToHandler handler) throws IOException {
    if (sqlTrace != null) {
      sqlTrace.query("COPY-OUT: " + sql);
    }
    submit(new CopyOutRequest(sql, stream, handler));
  }

  @Override
  public void close(ServerObjectType objectType, String objectName) throws IOException {
    submit(new CloseRequest(objectType, objectName, null));
  }

  @SuppressWarnings("RedundantThrows")
  private synchronized void submit(ServerRequest request) throws IOException {

    channel.writeAndFlush(request).syncUninterruptibly();
  }

}


class SQLTrace {

  private Writer out;
  private Map<String, String> preparedText;

  SQLTrace(Writer out) {
    this.out = out;
    this.preparedText = new HashMap<>();
  }

  void prepare(String statement, String text) {
    preparedText.put(statement, text);
    try {
      out.append("P: ").append(statement).append(" = ").append(text).append('\n').flush();
    }
    catch (IOException ignore) {
    }
  }

  void query(String text) {
    try {
      out.append("Q: ").append(text).append('\n').flush();
    }
    catch (IOException ignore) {
    }
  }

  void execute(String statement) {
    try {
      out.append("Q (").append(statement).append("): ").append(preparedText.get(statement)).append('\n').flush();
    }
    catch (IOException ignore) {
    }
  }

}
