package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBufAllocator;

public interface ServerConnection {

  ByteBufAllocator getAllocator();

  SocketAddress getRemoteAddress();

  TransactionStatus getTransactionStatus() throws IOException;

  RequestExecutor getRequestExecutor();

  CompletableFuture<?> shutdown();

  CompletableFuture<?> kill();

  boolean isConnected();

  ScheduledExecutorService getIOExecutor();

}
