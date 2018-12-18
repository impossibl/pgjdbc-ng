package com.impossibl.postgres.protocol;

import java.net.SocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBufAllocator;

public interface ServerConnection {

  ByteBufAllocator getAllocator();

  SocketAddress getRemoteAddress();

  TransactionStatus getTransactionStatus();

  RequestExecutor getRequestExecutor();

  Future<Void> shutdown();

  Future<Void> kill();

  boolean isConnected();

  ScheduledExecutorService getIOExecutor();

}
