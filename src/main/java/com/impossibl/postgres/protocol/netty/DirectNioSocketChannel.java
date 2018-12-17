package com.impossibl.postgres.protocol.netty;

import java.nio.channels.SocketChannel;

import io.netty.channel.socket.nio.NioSocketChannel;

public class DirectNioSocketChannel extends NioSocketChannel {

  @Override
  public SocketChannel javaChannel() {
    return super.javaChannel();
  }

}
