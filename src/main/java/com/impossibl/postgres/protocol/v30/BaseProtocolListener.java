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

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import java.io.IOException;

import static java.lang.Long.MAX_VALUE;

import io.netty.buffer.ByteBuf;

public class BaseProtocolListener implements ProtocolListener {

  private boolean aborted;

  @Override
  public synchronized void abort() {
    aborted = true;
    notifyAll();
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public boolean isComplete() {
    return true;
  }

  synchronized void notifyPossibleCompletion() {
    notifyAll();
  }

  @Override
  public void ready(TransactionStatus txStatus) throws IOException {
  }

  @Override
  public void parseComplete() throws IOException {
  }

  @Override
  public void parametersDescription(TypeRef[] parameterTypes) throws IOException {
  }

  @Override
  public void noData() throws IOException {
  }

  @Override
  public void bindComplete() throws IOException {
  }

  @Override
  public void rowDescription(ResultField[] asList) throws IOException {
  }

  @Override
  public void rowData(ByteBuf buffer) throws IOException {
  }

  @Override
  public void functionResult(Object value) throws IOException {
  }

  @Override
  public void emptyQuery() throws IOException {
  }

  @Override
  public void portalSuspended() throws IOException {
  }

  @Override
  public void commandComplete(String command, Long rowsAffected, Long oid) throws IOException {
  }

  @Override
  public void closeComplete() throws IOException {
  }

  @Override
  public void notification(int processId, String channelName, String payload) throws IOException {
  }

  @Override
  public void notice(Notice notice) throws IOException {
  }

  @Override
  public void error(Notice error) throws IOException {
  }

  @Override
  public void backendKeyData(int processId, int secretKey) throws IOException {
  }

  @Override
  public void authenticated(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateKerberos(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateClear(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateCrypt(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException {
  }

  @Override
  public void authenticateSCM(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateGSS(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateGSSCont(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void authenticateSSPI(ProtocolImpl protocol) throws IOException {
  }

  @Override
  public void exception(Throwable throwable) throws IOException {
  }

  @Override
  public synchronized void waitUntilComplete(long totalNetworkTimeout) throws IOException {

    long networkTimeout = totalNetworkTimeout;

    if (networkTimeout < 1) {
      networkTimeout = MAX_VALUE;
    }

    while (!isComplete() && !isAborted()) {

      long start = System.currentTimeMillis();

      try {

        wait(networkTimeout);

      }
      catch (InterruptedException e) {
        // Ignore
      }

      networkTimeout -= (System.currentTimeMillis() - start);

      if (networkTimeout < 1) {
        throw new BlockingReadTimeoutException("network timeout reached");
      }

    }

  }

}
