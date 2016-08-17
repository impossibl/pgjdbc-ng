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

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class DelegatedProtocolListener extends BaseProtocolListener {

  ProtocolListener delegate;

  public DelegatedProtocolListener(ProtocolListener delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isComplete() {
    return delegate.isComplete();
  }

  @Override
  public void parseComplete() throws IOException {
    delegate.parseComplete();
  }

  @Override
  public void parametersDescription(TypeRef[] parameterTypes) throws IOException {
    delegate.parametersDescription(parameterTypes);
  }

  @Override
  public void noData() throws IOException {
    delegate.noData();
  }

  @Override
  public void bindComplete() throws IOException {
    delegate.bindComplete();
  }

  @Override
  public void rowDescription(ResultField[] resultFields) throws IOException {
    delegate.rowDescription(resultFields);
  }

  @Override
  public void rowData(ByteBuf buffer) throws IOException {
    delegate.rowData(buffer);
  }

  @Override
  public void functionResult(Object value) throws IOException {
    delegate.functionResult(value);
  }

  @Override
  public void emptyQuery() throws IOException {
    delegate.emptyQuery();
  }

  @Override
  public void portalSuspended() throws IOException {
    delegate.portalSuspended();
  }

  @Override
  public void commandComplete(String command, Long rowsAffected, Long oid) throws IOException {
    delegate.commandComplete(command, rowsAffected, oid);
  }

  @Override
  public void closeComplete() throws IOException {
    delegate.closeComplete();
  }

  @Override
  public void ready(TransactionStatus txStatus) throws IOException {
    delegate.ready(txStatus);
  }

  @Override
  public void error(Notice error) throws IOException {
    delegate.error(error);
  }

  @Override
  public void notice(Notice notice) throws IOException {
    delegate.notice(notice);
  }

  @Override
  public void notification(int processId, String channelName, String payload) throws IOException {
    delegate.notification(processId, channelName, payload);
  }

  @Override
  public void backendKeyData(int processId, int secretKey) throws IOException {
    delegate.backendKeyData(processId, secretKey);
  }

  @Override
  public void authenticated(ProtocolImpl protocol) throws IOException {
    delegate.authenticated(protocol);
  }

  @Override
  public void authenticateKerberos(ProtocolImpl protocol) throws IOException {
    delegate.authenticateKerberos(protocol);
  }

  @Override
  public void authenticateClear(ProtocolImpl protocol) throws IOException {
    delegate.authenticateClear(protocol);
  }

  @Override
  public void authenticateCrypt(ProtocolImpl protocol) throws IOException {
    delegate.authenticateCrypt(protocol);
  }

  @Override
  public void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException {
    delegate.authenticateMD5(protocol, salt);
  }

  @Override
  public void authenticateSCM(ProtocolImpl protocol) throws IOException {
    delegate.authenticateSCM(protocol);
  }

  @Override
  public void authenticateGSS(ProtocolImpl protocol) throws IOException {
    delegate.authenticateGSS(protocol);
  }

  @Override
  public void authenticateGSSCont(ProtocolImpl protocol) throws IOException {
    delegate.authenticateGSSCont(protocol);
  }

  @Override
  public void authenticateSSPI(ProtocolImpl protocol) throws IOException {
    delegate.authenticateSSPI(protocol);
  }

  @Override
  public void exception(Throwable cause) throws IOException {
    delegate.exception(cause);
  }
}
