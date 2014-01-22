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
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface ProtocolListener {

  void abort();

  boolean isAborted();

  boolean isComplete();

  void parseComplete() throws IOException;

  void parametersDescription(List<TypeRef> parameterTypes) throws IOException;

  void noData() throws IOException;

  void bindComplete() throws IOException;

  void rowDescription(List<ResultField> resultFields) throws IOException;

  void rowData(ByteBuf buffer) throws IOException;

  void functionResult(Object value) throws IOException;

  void emptyQuery() throws IOException;

  void portalSuspended() throws IOException;

  void commandComplete(String command, Long rowsAffected, Long oid) throws IOException;

  void closeComplete() throws IOException;

  void ready(TransactionStatus txStatus) throws IOException;

  void error(Notice error) throws IOException;

  void notice(Notice notice) throws IOException;

  void notification(int processId, String channelName, String payload) throws IOException;

  void backendKeyData(int processId, int secretKey) throws IOException;

  void authenticated(ProtocolImpl protocol) throws IOException;

  void authenticateKerberos(ProtocolImpl protocol) throws IOException;

  void authenticateClear(ProtocolImpl protocol) throws IOException;

  void authenticateCrypt(ProtocolImpl protocol) throws IOException;

  void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException;

  void authenticateSCM(ProtocolImpl protocol) throws IOException;

  void authenticateGSS(ProtocolImpl protocol) throws IOException;

  void authenticateGSSCont(ProtocolImpl protocol) throws IOException;

  void authenticateSSPI(ProtocolImpl protocol) throws IOException;

  void exception(Throwable cause) throws IOException;
}
