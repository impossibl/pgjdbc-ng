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

import com.impossibl.postgres.system.Context;

import static com.impossibl.postgres.protocol.v30.BackendMessageDispatcher.backendDispatch;
import static com.impossibl.postgres.protocol.v30.BackendMessageDispatcher.isReadyForQuery;
import static com.impossibl.postgres.protocol.v30.ProtocolHandlers.SYNC;
import static com.impossibl.postgres.protocol.v30.ServerConnection.STATE_KEY;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;


public class IncomingMessageHandler extends ChannelInboundHandlerAdapter {

  private StringBuilder readEncoded;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {

    ServerConnection.State state = ctx.channel().attr(STATE_KEY).get();

    readEncoded = new StringBuilder();

    ByteBuf msg = (ByteBuf) message;
    try {
      // Parse message header

      byte id = msg.readByte();
      int length = msg.readInt() - 4;
      ByteBuf data = msg.readSlice(length);

      readEncoded.append((char) id).append('.');

      // Dispatch to current request handler

      ProtocolHandler protocolHandler = state.protocolHandlers.element();

      dispatch(ctx, id, data, state, protocolHandler);

    }
    finally {
      ReferenceCountUtil.release(msg);

      if (state.traceRequestProcessing) {
        System.out.print(readEncoded);
      }
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().attr(STATE_KEY).get().traceRequestProcessing) {
      System.out.println();
    }
    super.channelReadComplete(ctx);
  }

  private void dispatch(ChannelHandlerContext ctx, byte id, ByteBuf data, ServerConnection.State state, ProtocolHandler protocolHandler) {

    Context context = state.context.get();
    if (context == null) {
      return;
    }

    ProtocolHandler.Action action;
    try {
      action = backendDispatch(context, ctx.channel(), state, id, data, protocolHandler);
    }
    catch (IOException e) {
      try {
        protocolHandler.exception(e);
      }
      catch (IOException sub) {
        // Failing now will have no real effect, we always sync on pipeline exception
      }
      action = isReadyForQuery(id) ? ProtocolHandler.Action.Complete : ProtocolHandler.Action.Sync;
    }

    if (action == null) {
      String failMessage = "Unhandled message: " + (char)id + " @ " + protocolHandler.getClass().getName();
      throw new IllegalStateException(failMessage);
    }

    switch (action) {
      case Resume:
        // Nothing to do...
        readEncoded.append('.');
        break;

      case ResumePassing:
        readEncoded.append(".^");
        ProtocolHandler resume = state.protocolHandlers.pop();
        try {
          data.resetReaderIndex();
          dispatch(ctx, id, data, state, state.protocolHandlers.peek());
        }
        finally {
          state.protocolHandlers.addFirst(resume);
        }
        break;

      case Complete:
        readEncoded.append("*");
        state.protocolHandlers.pop();
        break;

      case CompletePassing:
        readEncoded.append("*^");
        state.protocolHandlers.pop();
        data.resetReaderIndex();
        dispatch(ctx, id, data, state, state.protocolHandlers.peek());
        break;

      case Sync:
        readEncoded.append('$');
        state.protocolHandlers.pop();
        state.protocolHandlers.addFirst(SYNC);
        break;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {

    // Dispatch to current request handler (if any)

    ServerConnection.State state = ctx.channel().attr(STATE_KEY).get();
    ProtocolHandler protocolHandler = state.protocolHandlers.poll();
    if (protocolHandler == null) return;

    try {
      protocolHandler.exception(new ClosedChannelException());
    }
    catch (IOException ignored) {
    }

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    // Dispatch to current request handler (if any)

    ServerConnection.State state = ctx.channel().attr(STATE_KEY).get();
    ProtocolHandler protocolHandler = state.protocolHandlers.poll();
    if (protocolHandler == null) return;

    try {
      protocolHandler.exception(cause);
    }
    catch (IOException ignored) {
    }

  }

}
