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
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.utils.MD5Authentication;

import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;

import java.io.IOException;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class StartupCommandImpl extends CommandImpl implements StartupCommand {

  Map<String, Object> params;
  boolean ready;

  public StartupCommandImpl(Map<String, Object> params) {
    this.params = params;
  }

  @Override
  public void execute(final ProtocolImpl protocol) throws IOException {

    ProtocolListener listener = new BaseProtocolListener() {

      @Override
      public boolean isComplete() {
        return ready || error != null || exception != null;
      }

      @Override
      public synchronized void ready(TransactionStatus txStatus) {
        StartupCommandImpl.this.ready = true;
        notifyAll();
      }

      @Override
      public synchronized void error(Notice error) {
        setError(error);
        notifyAll();
      }

      @Override
      public synchronized void exception(Throwable cause) {
        setException(cause);
        notifyAll();
      }

      @Override
      public void notice(Notice notice) {
        addNotice(notice);
      }

      @Override
      public void backendKeyData(int processId, int secretKey) {
        protocol.getContext().setKeyData(processId, secretKey);
      }

      @Override
      public void authenticated(ProtocolImpl protocol) {
      }

      @Override
      public void authenticateKerberos(ProtocolImpl protocol) {
      }

      @Override
      public void authenticateClear(ProtocolImpl protocol) throws IOException {

        Context context = protocol.getContext();

        String password = context.getSetting(CREDENTIALS_PASSWORD).toString();

        ByteBuf msg = protocol.channel.alloc().buffer();

        protocol.writePassword(msg, password);

        protocol.send(msg);
      }

      @Override
      public void authenticateCrypt(ProtocolImpl protocol) throws IOException {
      }

      @Override
      public void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException {

        Context context = protocol.getContext();

        String username = context.getSetting(CREDENTIALS_USERNAME).toString();
        String password = context.getSetting(CREDENTIALS_PASSWORD).toString();

        String response = MD5Authentication.encode(password, username, salt);

        ByteBuf msg = protocol.channel.alloc().buffer();

        protocol.writePassword(msg, response);

        protocol.send(msg);
      }

      @Override
      public void authenticateSCM(ProtocolImpl protocol) {
      }

      @Override
      public void authenticateGSS(ProtocolImpl protocol) {
      }

      @Override
      public void authenticateGSSCont(ProtocolImpl protocol) {
      }

      @Override
      public void authenticateSSPI(ProtocolImpl protocol) {
      }

    };

    protocol.setListener(listener);

    ByteBuf msg = protocol.channel.alloc().buffer();

    protocol.writeStartup(msg, params);

    protocol.send(msg);

    waitFor(listener);

  }

}
