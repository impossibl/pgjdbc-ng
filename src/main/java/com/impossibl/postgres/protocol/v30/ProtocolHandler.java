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
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.TypeRef;

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface ProtocolHandler {

  void exception(Throwable cause) throws IOException;


  enum Action {
    Resume,
    ResumePassing,
    Complete,
    CompletePassing,
    Sync,
  }

  interface NegotiateProtocolVersion extends ProtocolHandler {

    Action negotiate(int maxSupportedMinorVersion, List<String> unrecognizedParameters) throws IOException;

  }

  interface Authentication extends ProtocolHandler {

    Action authenticated() throws IOException;
    void authenticateKerberos(ProtocolChannel channel) throws IOException;
    void authenticateClear(ProtocolChannel channel) throws IOException;
    void authenticateMD5(byte[] salt, ProtocolChannel channel) throws IOException;
    void authenticateSCM(ProtocolChannel channel) throws IOException;
    void authenticateGSS(ByteBuf data, ProtocolChannel channel) throws IOException;
    void authenticateSSPI(ByteBuf data, ProtocolChannel channel) throws IOException;
    void authenticateContinue(ByteBuf data, ProtocolChannel channel) throws IOException;

  }

  interface BackendKeyData extends ProtocolHandler {

    Action backendKeyData(int processId, int secretKey) throws IOException;

  }

  interface ParameterStatus extends ProtocolHandler {

    Action parameterStatus(String name, String value) throws IOException;

  }

  interface ParameterDescriptions extends ProtocolHandler {

    Action parameterDescriptions(TypeRef[] types) throws IOException;

  }

  interface RowDescription extends ProtocolHandler {

    Action rowDescription(ResultField[] fields) throws IOException;

  }

  interface DataRow extends ProtocolHandler {

    Action rowData(ByteBuf data) throws IOException;

  }

  interface PortalSuspended extends ProtocolHandler {

    Action portalSuspended() throws IOException;

  }

  interface NoData extends ProtocolHandler {

    Action noData() throws IOException;

  }

  interface ParseComplete extends ProtocolHandler {

    Action parseComplete() throws IOException;

  }

  interface BindComplete extends ProtocolHandler {

    Action bindComplete() throws IOException;

  }

  interface CloseComplete extends ProtocolHandler {

    Action closeComplete() throws IOException;

  }

  interface EmptyQuery extends ProtocolHandler {

    Action emptyQuery() throws IOException;

  }

  interface FunctionResult extends ProtocolHandler {

    Action functionResult(ByteBuf data) throws IOException;

  }

  interface CommandError extends ProtocolHandler {

    Action error(Notice notice) throws IOException;

  }

  interface ReportNotice extends ProtocolHandler {

    Action notice(Notice notice) throws IOException;

  }

  interface CommandComplete extends ProtocolHandler {

    Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException;

  }

  interface ReadyForQuery extends ProtocolHandler {

    Action readyForQuery(TransactionStatus txnStatus) throws IOException;

  }

  interface Notification {

    void notification(int processId, String channelName, String payload) throws IOException;

  }

}
