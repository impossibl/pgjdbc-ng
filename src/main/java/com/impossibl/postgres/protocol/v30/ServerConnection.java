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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;


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

    //Ensure only one thread can ever succeed in calling shutdown
    if (!channel.isActive()) {
      return channel.pipeline().newSucceededFuture();
    }

    // Stop reading while we are shutting down...
    channel.config().setOption(ChannelOption.AUTO_READ, false);

    try {
      return new ProtocolChannel(channel, StandardCharsets.UTF_8)
          .writeTerminate()
          .addListener(ChannelFutureListener.CLOSE);
    }
    catch (Exception ignore) {
    }

    sharedRef.release();

    return kill();
  }

  @Override
  public ChannelFuture kill() {

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
  public void query(String sql, RequestExecutor.SimpleQueryHandler handler) throws IOException {
    submit(new QueryRequest(sql, handler));
  }

  @Override
  public void query(String sql, String portalName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats, int maxRows, RequestExecutor.QueryHandler handler) throws IOException {
    submit(new ExecuteQueryRequest(sql, portalName, parameterFormats, parameterBuffers, resultFieldFormats, maxRows, handler));
  }

  @Override
  public void prepare(String statementName, String sqlText, Type[] parameterTypes, RequestExecutor.PrepareHandler handler) throws IOException {
    submit(new PrepareRequest(statementName, sqlText, parameterTypes, handler));
  }

  @Override
  public void execute(String portalName, String statementName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormatRefs, int maxRows, ExecuteHandler handler) throws IOException {
    submit(new ExecuteStatementRequest(statementName, portalName, parameterFormats, parameterBuffers, resultFieldFormatRefs, maxRows, handler));
  }

  @Override
  public void resume(String portalName, int maxRows, ExecuteHandler handler) throws IOException {
    submit(new ResumePortalRequest(portalName, maxRows, handler));
  }

  @Override
  public void lazyExecute(String statementName) throws IOException {
    submit(new LazyExecuteRequest(statementName));
  }

  @Override
  public void call(int functionId, FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers, RequestExecutor.FunctionCallHandler handler) throws IOException {
    submit(new FunctionCallRequest(functionId, parameterFormatRefs, parameterBuffers, handler));
  }

  @Override
  public void close(ServerObjectType objectType, String objectName) throws IOException {
    submit(new CloseRequest(objectType, objectName));
  }

  synchronized void submit(ServerRequest request) {

    channel.writeAndFlush(request, channel.voidPromise());
  }

}
