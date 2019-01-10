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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


class SSLQueryRequest implements ServerRequest {

  private AtomicBoolean allowed;
  private CountDownLatch completed;

  SSLQueryRequest() {
    this.allowed = new AtomicBoolean(false);
    this.completed = new CountDownLatch(1);
  }

  public boolean isAllowed() {
    return allowed.get();
  }

  boolean await(long timeout, TimeUnit timeoutUnits) throws InterruptedException {
    return completed.await(timeout, timeoutUnits);
  }

  @Override
  public ProtocolHandler createHandler() {
    return null;
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    // Add special channel handler to intercept SSL request responses
    // and automatically remove it once the query is complete.

    channel.pipeline().addFirst("ssl-query", new SimpleChannelInboundHandler<ByteBuf>() {

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        try {

          switch (msg.readByte()) {
            case 'S':
              allowed.set(true);
              break;

            case 'N':
              allowed.set(false);
              break;

            default:
              throw new IOException("invalid SSL response");
          }

        }
        finally {

          // Remove the handler immediately
          channel.pipeline().remove("ssl-query");

          completed.countDown();
        }

      }

    });

    channel
        .writeSSLRequest()
        .flush();

  }

}
