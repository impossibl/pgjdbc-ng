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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.ServerInfo;
import com.impossibl.postgres.system.Version;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;

public interface ServerConnection {

  interface Listener {
    void parameterStatusChanged(String name, String value);
    void notificationReceived(int processId, String channelName, String payload);
  }

  class KeyData {
    private int processId;
    private int secretKey;

    public KeyData(int processId, int secretKey) {
      this.processId = processId;
      this.secretKey = secretKey;
    }

    public int getProcessId() {
      return processId;
    }

    public int getSecretKey() {
      return secretKey;
    }
  }

  ServerInfo getServerInfo();

  Version getProtocolVersion();

  KeyData getKeyData();

  ByteBufAllocator getAllocator();

  SocketAddress getRemoteAddress();

  TransactionStatus getTransactionStatus() throws IOException;

  RequestExecutor getRequestExecutor();

  ChannelFuture shutdown();

  ChannelFuture kill();

  boolean isConnected();

  ScheduledExecutorService getIOExecutor();

}
