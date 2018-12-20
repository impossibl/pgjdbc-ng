package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.ALLOCATOR;
import static com.impossibl.postgres.system.Settings.ALLOCATOR_DEFAULT;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE_DEFAULT;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE_DEFAULT;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.SocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Boolean.parseBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;


class ServerConnection implements com.impossibl.postgres.protocol.ServerConnection, RequestExecutor {

  static final AttributeKey<ServerConnection> STATE_KEY = AttributeKey.valueOf("state");

  private BasicContext owner;
  private Channel channel;
  private TransactionStatus transactionStatus;
  private ProtocolHandler.Notification notificationHandler;
  private AtomicBoolean connected;
  private ServerConnectionShared.Ref sharedRef;


  ServerConnection(BasicContext owner, Channel channel, ServerConnectionShared.Ref sharedRef) {
    this.owner = owner;
    this.channel = channel;
    this.transactionStatus = TransactionStatus.Idle;
    this.notificationHandler = owner::reportNotification;
    this.connected = new AtomicBoolean(true);
    this.sharedRef = sharedRef;

    ///
    // Configure channel
    //

    // Shared handler state
    channel.attr(STATE_KEY).set(this);

    // Set allocator
    boolean usePooledAllocator = parseBoolean(owner.getSetting(ALLOCATOR, ALLOCATOR_DEFAULT));
    channel.config().setOption(
        ChannelOption.ALLOCATOR,
        usePooledAllocator ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT
    );

    // Configure socket options (if provided)
    if (owner.getSetting(RECEIVE_BUFFER_SIZE, RECEIVE_BUFFER_SIZE_DEFAULT) != RECEIVE_BUFFER_SIZE_DEFAULT) {
      channel.config().setOption(ChannelOption.SO_RCVBUF, owner.getSetting(RECEIVE_BUFFER_SIZE, int.class));
    }
    if (owner.getSetting(SEND_BUFFER_SIZE, SEND_BUFFER_SIZE_DEFAULT) != SEND_BUFFER_SIZE_DEFAULT) {
        channel.config().setOption(ChannelOption.SO_SNDBUF, owner.getSetting(SEND_BUFFER_SIZE, int.class));
    }

    if (parseBoolean(owner.getSetting(PROTOCOL_TRACE, PROTOCOL_TRACE_DEFAULT))) {
      MessageDispatchHandler dispatchHandler =
          (MessageDispatchHandler) channel.pipeline().context(MessageDispatchHandler.class).handler();
      dispatchHandler.setTraceWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
    }

  }

  private EventLoop eventLoop() {
    return channel.eventLoop();
  }

  ChannelPipeline pipeline() {
    return channel.pipeline();
  }

  ProtocolHandler.Notification getNotificationHandler() {
    return notificationHandler;
  }

  @Override
  public Future<Void> shutdown() {

    //Ensure only one thread can ever succeed in calling shutdown
    if (!connected.getAndSet(false)) {
      return eventLoop().newSucceededFuture(null);
    }

    // Stop reading while we are shutting down...
    channel.config().setOption(ChannelOption.AUTO_READ, false);

    try {
      return new ProtocolChannel(channel, getOwner().getCharset())
          .writeTerminate()
          .addListener(ChannelFutureListener.CLOSE);
    }
    catch (Exception e) {
      //Close anyway...

      return channel.close();
    }
    finally {
      sharedRef.release();
    }
  }

  @Override
  public Future<Void> kill() {

    if (!connected.getAndSet(false)) {
      return eventLoop().newSucceededFuture(null);
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
  public TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  void setTransactionStatus(TransactionStatus transactionStatus) {
    this.transactionStatus = transactionStatus;
  }

  @Override
  public boolean isConnected() {
    return connected.get();
  }

  public BasicContext getOwner() {
    return owner;
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

  synchronized void submit(ServerRequest request) throws IOException {

    if (!connected.get()) {
      throw new IOException("Connection closed");
    }

    // Add handler to queue (if request produces one)

    ProtocolHandler requestProtocolHandler = request.createHandler();
    if (requestProtocolHandler != null) {

      channel.write(requestProtocolHandler, channel.voidPromise());

    }

    // Execute the request

    request.execute(new ProtocolChannel(channel, owner.getCharset()));
  }

}
