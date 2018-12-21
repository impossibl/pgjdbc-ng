package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;

public interface ServerConnection {

  ByteBufAllocator getAllocator();

  SocketAddress getRemoteAddress();

  TransactionStatus getTransactionStatus() throws IOException;

  RequestExecutor getRequestExecutor();

  ChannelFuture shutdown();

  ChannelFuture kill();

  boolean isConnected();

  ScheduledExecutorService getIOExecutor();

}
