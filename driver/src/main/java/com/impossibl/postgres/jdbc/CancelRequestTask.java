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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.protocol.ServerConnection;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import io.netty.channel.unix.DomainSocketAddress;

public class CancelRequestTask extends ExecutionTimerTask {

  private static final Logger logger = Logger.getLogger(CancelRequestTask.class.getName());

  private SocketAddress serverAddress;
  private ServerConnection.KeyData keyData;

  CancelRequestTask(SocketAddress serverAddress, ServerConnection.KeyData keyData) {
    this.serverAddress = serverAddress;
    this.keyData = keyData;
  }

  @Override
  public void go() {
    sendCancelRequest();
  }

  private void sendCancelRequest() {

    if (keyData.getProcessId() == 0 && keyData.getSecretKey() == 0) {
      logger.warning("Cannot send CancelRequest because of missing BackendKeyData.");
      return;
    }

    try {

      if (serverAddress instanceof InetSocketAddress) {

        InetSocketAddress target = (InetSocketAddress) serverAddress;

        try (Socket abortSocket = new Socket(target.getAddress(), target.getPort())) {
          writeCancelRequest(new DataOutputStream(abortSocket.getOutputStream()));
        }

      }
      else if (serverAddress instanceof DomainSocketAddress) {

        io.netty.channel.unix.Socket unixSocket = io.netty.channel.unix.Socket.newSocketDomain();
        try {
          unixSocket.connect(serverAddress);

          try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {

            writeCancelRequest(new DataOutputStream(os));

            byte[] request = os.toByteArray();
            ByteBuffer buffer = ByteBuffer.allocateDirect(request.length);
            buffer.put(request, 0, request.length);
            buffer.flip();

            unixSocket.write(buffer, 0, buffer.limit());
          }

        }
        finally {
          unixSocket.close();
        }

      }

    }
    catch (IOException ignored) {
      // All exceptions during a cancellation attempt are ignored...
    }

  }

  private void writeCancelRequest(DataOutputStream os) throws IOException {

    os.writeInt(16);
    os.writeInt(80877102);
    os.writeInt(keyData.getProcessId());
    os.writeInt(keyData.getSecretKey());
  }

}
