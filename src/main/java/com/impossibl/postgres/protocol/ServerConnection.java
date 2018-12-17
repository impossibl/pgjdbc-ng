package com.impossibl.postgres.protocol;

import java.util.concurrent.Future;

import io.netty.channel.Channel;

public interface ServerConnection {

  Channel getChannel();

  TransactionStatus getTransactionStatus();

  RequestExecutor getRequestExecutor();

  Future<Void> shutdown();

  Future<Void> kill();

  boolean isConnected();

}
