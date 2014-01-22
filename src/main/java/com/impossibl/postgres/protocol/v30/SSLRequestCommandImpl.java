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

import com.impossibl.postgres.protocol.SSLRequestCommand;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class SSLRequestCommandImpl extends CommandImpl implements SSLRequestCommand {

  Boolean allowed;
  boolean completed;

  @Override
  public boolean isAllowed() {
    return allowed;
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    try {

      // Add special channel handler to intercept SSL request responses
      protocol.channel.pipeline().addFirst("ssl-query", new SimpleChannelInboundHandler<ByteBuf>() {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
          try {

            completed = true;

            switch (msg.readByte()) {
              case 'S':
                allowed = true;
                break;

              case 'N':
                allowed = false;
                break;

              default:
                throw new IOException("invalid SSL response");
            }

          }
          finally {
            synchronized (SSLRequestCommandImpl.this) {
              SSLRequestCommandImpl.this.notifyAll();
            }
          }

        }

      });

      ByteBuf msg = protocol.channel.alloc().buffer();

      protocol.writeSSLRequest(msg);

      protocol.send(msg);

      synchronized (this) {

        while (!completed) {

          try {

            this.wait();

          }
          catch (InterruptedException e) {
            // Ignore
          }

        }

      }

    }
    finally {
      protocol.channel.pipeline().remove("ssl-query");
    }

  }

}
