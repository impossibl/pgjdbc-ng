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
package com.impossibl.postgres.jdbc.util;

import com.impossibl.postgres.utils.guava.Preconditions;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;

/**
 * Based off of the example proxy included in Netty.
 *
 * https://github.com/netty/netty/tree/master/example/src/main/java/io/netty/example/proxy
 */
public class TcpProxyServer {

  private final ServerBootstrap bootstrap;
  private int port = -1;

  public TcpProxyServer(SslContext context, String remoteHost, int remotePort) {
    this.bootstrap = new ServerBootstrap();
    bootstrap.channel(NioServerSocketChannel.class)
        .group(new NioEventLoopGroup(1))
        .childHandler(new ProxyInitializer(context, remoteHost, remotePort))
        .childOption(ChannelOption.AUTO_READ, false);
  }

  public void start() throws InterruptedException {
    // Use 0 to let the system pick a port.
    Channel ch = bootstrap.bind(0).sync().channel();
    port = ((InetSocketAddress) ch.localAddress()).getPort();
  }

  public int port() {
    Preconditions.checkState(port > 0, "The proxy must be started first.");
    return port;
  }

  public void shutdownGracefully() {
    bootstrap.config().group().shutdownGracefully();
  }

  private class ProxyInitializer extends ChannelInitializer<SocketChannel> {
    private final SslContext context;
    private final String remoteHost;
    private final int remotePort;

    ProxyInitializer(SslContext context, String remoteHost, int remotePort) {
      this.context = context;
      this.remoteHost = remoteHost;
      this.remotePort = remotePort;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
      ch.pipeline().addLast(context.newHandler(ch.alloc()));
      ch.pipeline().addLast(new ProxyFrontendHandler(remoteHost, remotePort));
    }
  }

  private class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {
    private final String remoteHost;
    private final int remotePort;

    private Channel outboundChannel;

    ProxyFrontendHandler(String remoteHost, int remotePort) {
      this.remoteHost = remoteHost;
      this.remotePort = remotePort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      final Channel inboundChannel = ctx.channel();

      // Start the connection attempt.
      Bootstrap b = new Bootstrap();
      b.group(inboundChannel.eventLoop())
          .channel(ctx.channel().getClass())
          .handler(new ProxyBackendHandler(inboundChannel))
          .option(ChannelOption.AUTO_READ, false);
      ChannelFuture f = b.connect(remoteHost, remotePort);
      outboundChannel = f.channel();
      f.addListener((ChannelFutureListener) future -> {
        if (future.isSuccess()) {
          // connection complete start to read first data
          inboundChannel.read();
        }
        else {
          // Close the connection if the connection attempt has failed.
          inboundChannel.close();
        }
      });
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      if (outboundChannel.isActive()) {
        outboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
          if (future.isSuccess()) {
            // was able to flush out data, start to read the next chunk
            ctx.channel().read();
          }
          else {
            future.channel().close();
          }
        });
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (outboundChannel != null) {
        closeOnFlush(outboundChannel);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      closeOnFlush(ctx.channel());
    }
  }

  private class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private final Channel inboundChannel;

    ProxyBackendHandler(Channel inboundChannel) {
      this.inboundChannel = inboundChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
        if (future.isSuccess()) {
          ctx.channel().read();
        }
        else {
          future.channel().close();
        }
      });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      closeOnFlush(ctx.channel());
    }
  }


  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  private static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
