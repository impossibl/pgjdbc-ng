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
package com.impossibl.postgres.protocol.v30;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.Future;

public class ProtocolShared {

  public class Ref {

    private boolean released;

    public ProtocolShared get() {
      return ProtocolShared.this;
    }

    public void release() {
      if (!released) {
        released = true;
        ProtocolShared.this.release();
      }
    }
  }

  static ProtocolShared instance;

  public static synchronized Ref acquire() {
    if (instance == null) {
      instance = new ProtocolShared();
    }
    return instance.addReference();
  }

  private Bootstrap bootstrap;
  private int count = 0;

  public Bootstrap getBootstrap() {
    return bootstrap;
  }

  private synchronized Ref addReference() {
    if (count == 0) {
      init();
    }
    count++;
    return new Ref();
  }

  private synchronized void release() {
    if (count == 1) {
      shutdown();
      count = 0;
    }
    else {
      count--;
    }
  }

  private void init() {
    int workerCount = getRuntime().availableProcessors();
    NioEventLoopGroup group = new NioEventLoopGroup(workerCount, new NamedThreadFactory("PG-JDBC EventLoop"));

    bootstrap = new Bootstrap();
    bootstrap.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new MessageDecoder(), new MessageHandler());
      }
    }).option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  public Future<?> shutdown() {

    return bootstrap.group().shutdownGracefully(10, 100, TimeUnit.MILLISECONDS);
  }

  public void waitForShutdown() {

    shutdown().awaitUninterruptibly();

    try {
      ThreadDeathWatcher.awaitInactivity(30, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      // Ignore
    }

  }

}


class NamedThreadFactory implements ThreadFactory {

  private String baseName;
  private AtomicInteger idx = new AtomicInteger(1);

  public NamedThreadFactory(String baseName) {
    super();
    this.baseName = baseName;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r, baseName + " (" + idx.getAndIncrement() + ")");
    thread.setDaemon(true);
    if (thread.getPriority() != Thread.NORM_PRIORITY)
      thread.setPriority(Thread.NORM_PRIORITY);
    return thread;
  }

}
