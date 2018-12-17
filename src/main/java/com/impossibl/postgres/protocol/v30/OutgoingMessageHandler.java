package com.impossibl.postgres.protocol.v30;

import static com.impossibl.postgres.protocol.v30.ServerConnection.STATE_KEY;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;


public class OutgoingMessageHandler extends ChannelOutboundHandlerAdapter {

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ByteBuf buffer = (ByteBuf) msg;

    if (ctx.channel().attr(STATE_KEY).get().traceRequestProcessing) {
      System.out.append((char)buffer.getByte(0)).append('.');
    }

    super.write(ctx, msg, promise);
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {

    if (ctx.channel().attr(STATE_KEY).get().traceRequestProcessing) {
      System.out.append('\n');
    }

    super.flush(ctx);
  }
}
