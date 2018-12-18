package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.FieldFormats.REQUEST_ALL_TEXT;
import static com.impossibl.postgres.system.Settings.ALLOCATOR;
import static com.impossibl.postgres.system.Settings.ALLOCATOR_DEFAULT;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.RECEIVE_BUFFER_SIZE_DEFAULT;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.Settings.SEND_BUFFER_SIZE_DEFAULT;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Boolean.parseBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;


class ServerConnection implements com.impossibl.postgres.protocol.ServerConnection, RequestExecutor {

  static final AttributeKey<ServerConnection.State> STATE_KEY = AttributeKey.valueOf("state");

  static class State {

    boolean traceRequestProcessing = false;
    WeakReference<Context> context;
    TransactionStatus transactionStatus = TransactionStatus.Idle;
    ConcurrentLinkedDeque<ProtocolHandler> protocolHandlers = new ConcurrentLinkedDeque<>();
    ProtocolHandler.Notification notificationHandler;

    State(Context context, ProtocolHandler.Notification notificationHandler) {
      this.context = new WeakReference<>(context);
      this.notificationHandler = notificationHandler;
    }

  }

  private Channel channel;
  private State state;
  private AtomicBoolean connected;
  private ServerConnectionShared.Ref sharedRef;


  ServerConnection(BasicContext owner, Channel channel, ServerConnectionShared.Ref sharedRef) {
    this.channel = channel;
    this.state = new State(owner, owner::reportNotification);
    this.connected = new AtomicBoolean(true);
    this.sharedRef = sharedRef;

    ///
    // Configure channel
    //

    // Shared handler state
    channel.attr(STATE_KEY).set(state);

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

  }

  EventLoop eventLoop() {
    return channel.eventLoop();
  }

  ChannelPipeline pipeline() {
    return channel.pipeline();
  }

  @Override
  public Future<Void> shutdown() {

    //Ensure only one thread can ever succeed in calling shutdown
    if (!connected.getAndSet(false)) {
      return eventLoop().newSucceededFuture(null);
    }

    sharedRef.release();

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
  }

  @Override
  public Future<Void> kill() {

    if (!connected.getAndSet(false)) {
      return eventLoop().newSucceededFuture(null);
    }

    return channel.close();
  }

  @Override
  public TransactionStatus getTransactionStatus() {
    return state.transactionStatus;
  }

  @Override
  public boolean isConnected() {
    return connected.get();
  }

  public Context getOwner() {
    return state.context.get();
  }

  @Override
  public RequestExecutor getRequestExecutor() {
    return this;
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public void query(String sql, RequestExecutor.QueryHandler handler) throws IOException {
    submit(new QueryRequest(sql, handler));
  }

  @Override
  public void query(String sql, String portalName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, RequestExecutor.QueryHandler handler) throws IOException {
    submit(new ExecuteQueryRequest(sql, portalName, parameterFormats, parameterBuffers, REQUEST_ALL_TEXT, handler));
  }

  @Override
  public void prepare(String statementName, String sqlText, Type[] parameterTypes, RequestExecutor.PrepareHandler handler) throws IOException {
    submit(new PrepareRequest(statementName, sqlText, parameterTypes, handler));
  }

  @Override
  public void execute(String portalName, String statementName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormatRefs, Integer maxRows, ExecuteHandler handler) throws IOException {
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

    Context context = state.context.get();
    if (context == null) {
      // Not sure how we're still alive...
      return;
    }

    // Add handler to queue (if request produces one)

    ProtocolHandler requestProtocolHandler = request.createHandler();
    if (requestProtocolHandler != null) {

      state.protocolHandlers.offer(requestProtocolHandler);

    }

    // Execute the request

    request.execute(new ProtocolChannel(channel, context.getCharset()));
  }

}
