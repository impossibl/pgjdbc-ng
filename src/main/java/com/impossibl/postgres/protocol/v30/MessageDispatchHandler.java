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

import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import static com.impossibl.postgres.protocol.v30.BackendMessageDispatcher.backendDispatch;
import static com.impossibl.postgres.protocol.v30.BackendMessageDispatcher.isReadyForQuery;
import static com.impossibl.postgres.protocol.v30.ProtocolHandlers.SYNC;
import static com.impossibl.postgres.protocol.v30.ServerConnection.STATE_KEY;

import java.io.IOException;
import java.io.Writer;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;


public class MessageDispatchHandler extends ChannelDuplexHandler {

  private Deque<ProtocolHandler> protocolHandlers = new LinkedBlockingDeque<>();
  private Writer traceWriter;

  public void setTraceWriter(Writer traceWriter) {
    this.traceWriter = traceWriter;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

    if (msg instanceof ProtocolHandler) {

      // Instruction to enqueue a new protocol handler

      protocolHandlers.offer((ProtocolHandler) msg);
    }
    else if (msg instanceof ByteBuf) {

      // Write msg (after tracing it)

      ByteBuf buf = (ByteBuf) msg;

      trace('<');
      trace((char) buf.getByte(0));

      ctx.write(msg);
    }

  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    trace('\n');
    flushTrace();
    super.flush(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {

    ServerConnection serverConnection = ctx.channel().attr(STATE_KEY).get();

    ByteBuf msg = (ByteBuf) message;
    try {
      // Parse message header

      byte id = msg.readByte();
      int length = msg.readInt() - 4;
      ByteBuf data = msg.readSlice(length);

      trace('>');
      trace((char) id);

      // Dispatch to current request handler

      ProtocolHandler protocolHandler = protocolHandlers.element();

      dispatch(ctx, id, data, serverConnection, protocolHandler);

    }
    finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    trace('\n');
    flushTrace();
    super.channelReadComplete(ctx);
  }

  private void dispatch(ChannelHandlerContext ctx, byte id, ByteBuf data, ServerConnection serverConnection, ProtocolHandler protocolHandler) {

    BasicContext context = serverConnection.getOwner();
    if (context == null) {
      return;
    }

    ProtocolHandler.Action action;
    try {
      action = backendDispatch(context, ctx.channel(), serverConnection, id, data, protocolHandler);
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
        trace('.');
        break;

      case ResumePassing:
        trace(".^");
        ProtocolHandler resume = protocolHandlers.pop();
        try {
          data.resetReaderIndex();
          dispatch(ctx, id, data, serverConnection, protocolHandlers.peek());
        }
        finally {
          protocolHandlers.addFirst(resume);
        }
        break;

      case Complete:
        trace('*');
        protocolHandlers.pop();
        break;

      case CompletePassing:
        trace("*^");
        protocolHandlers.pop();
        data.resetReaderIndex();
        dispatch(ctx, id, data, serverConnection, protocolHandlers.peek());
        break;

      case Sync:
        trace("$");
        protocolHandlers.pop();
        protocolHandlers.addFirst(SYNC);
        break;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {

    ctx.channel().attr(STATE_KEY).get().shutdown();

    // Dispatch to current request handler (if any)

    ProtocolHandler protocolHandler;
    while ((protocolHandler = protocolHandlers.poll()) != null) {
      try {
        protocolHandler.exception(new BlockingReadTimeoutException());
      }
      catch (IOException ignored) {
        // No need to report here...
      }
    }

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    // Dispatch to current request handler (if any)

    ProtocolHandler protocolHandler = protocolHandlers.poll();
    if (protocolHandler == null) return;

    try {
      protocolHandler.exception(cause);
    }
    catch (IOException ignored) {
    }

  }

  private void flushTrace() {
    if (traceWriter == null) return;
    try {
      traceWriter.flush();
    }
    catch (IOException ignored) {
    }
  }

  private void trace(char id) {
    if (traceWriter == null) return;
    try {
      traceWriter.append(id);
    }
    catch (IOException ignored) {
    }
  }

  private void trace(CharSequence code) {
    if (traceWriter == null) return;
    try {
      traceWriter.append(code);
    }
    catch (IOException ignored) {
    }
  }

}
