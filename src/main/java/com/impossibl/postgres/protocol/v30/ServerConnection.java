package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.DATABASE_URL;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.Settings.PROTOCOL_TRACE_DEFAULT;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;


class ServerConnection implements com.impossibl.postgres.protocol.ServerConnection, RequestExecutor, HandlerContext {

  private Socket socket;
  private MessageDispatchHandler messageDispatchHandler;
  private ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
  private final ServerConnectionShared.Ref sharedRef;
  private Thread readThread;

  ServerConnection(Context context, Socket channel, ProtocolHandler defaultHandler, ServerConnectionShared.Ref sharedRef) {
    this.socket = channel;
    this.messageDispatchHandler = new MessageDispatchHandler(socket, context.getCharset(), defaultHandler);
    this.sharedRef = sharedRef;
    this.readThread = new Thread(this::reader);
    this.readThread.setName("PG-JDBC I/O - " + context.getSetting(DATABASE_URL));
    this.readThread.start();

    if (context.getSetting(PROTOCOL_TRACE, PROTOCOL_TRACE_DEFAULT)) {
      this.messageDispatchHandler.setTraceWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
    }
  }

  private DataInputStream socketIn;

  private void reader() {

    try {
      socketIn = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 32 * 1024));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    while (!socket.isClosed()) {
      try {

        byte id = socketIn.readByte();
        int length = socketIn.readInt();
        int msgLength = length + 1;

        ByteBuf msg = alloc.heapBuffer(msgLength);
        msg.writeByte(id);
        msg.writeInt(length);
        while (msg.readableBytes() < msgLength) {
          try {
            msg.writeBytes(socketIn, msgLength - msg.readableBytes());
          }
          catch (InterruptedIOException ignore) {
          }
        }

        try {
          messageDispatchHandler.channelRead(this, msg);
        }
        catch (Throwable t) {
          messageDispatchHandler.exceptionCaught(this, t);
        }

      }
      catch (EOFException e) {
        messageDispatchHandler.channelReadComplete(this);
      }
      catch (SocketException | ClosedChannelException e) {
        // Retest
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public CompletableFuture<?> shutdown() {

    //Ensure only one thread can ever succeed in calling shutdown
    if (socket.isClosed()) {
      return CompletableFuture.completedFuture(null);
    }

    try {
      // Stop reading while we are shutting down...
      socket.shutdownInput();

      return new ProtocolChannel(this, StandardCharsets.UTF_8)
          .writeTerminate()
          .whenCompleteAsync((o, throwable) -> kill(), getIOExecutor());

    }
    catch (Exception ignore) {
      return kill();
    }

  }

  @Override
  public CompletableFuture<?> kill() {

    if (socket.isClosed()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<?> res = new CompletableFuture<>();
    try {
      socket.close();
      res.complete(null);
    }
    catch (Exception e) {
      res.completeExceptionally(e);
    }

    readThread.interrupt();

    sharedRef.release();

    return res;
  }

  @Override
  public ByteBufAllocator getAllocator() {
    return alloc;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return socket.getRemoteSocketAddress();
  }

  @Override
  public ScheduledExecutorService getIOExecutor() {
    return sharedRef.get().getIOExecutor();
  }

  @Override
  public TransactionStatus getTransactionStatus() throws IOException {
    if (socket.isClosed()) {
      throw new ClosedChannelException();
    }
    return messageDispatchHandler.getTransactionStatus();
  }

  @Override
  public boolean isConnected() {
    return !socket.isClosed() && socket.isConnected();
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

    messageDispatchHandler.write(this, request, voidPromise());
  }

  @Override
  public ByteBufAllocator alloc() {
    return alloc;
  }

  private static CompletableFuture<?> voidPromise = CompletableFuture.completedFuture(null);
  @Override
  public CompletableFuture<?> voidPromise() {
    return voidPromise;
  }

  @Override
  public CompletableFuture<?> write(ByteBuf msg, CompletableFuture<?> promise) {

    try {
      messageDispatchHandler.write(this, msg, promise);
    }
    catch (Throwable t) {
      promise.completeExceptionally(t);
    }

    return promise;
  }

  @Override
  public void flush() throws IOException {

    messageDispatchHandler.flush(this);
  }

}
