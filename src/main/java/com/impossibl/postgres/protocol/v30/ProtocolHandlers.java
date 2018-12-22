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
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CloseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.FunctionResult;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NoData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterDescriptions;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.RowDescription;
import com.impossibl.postgres.protocol.TypeRef;

import io.netty.buffer.ByteBuf;

class ProtocolHandlers {

  static final ProtocolHandler SYNC = new Adapter() {

    public String toString() {
      return "SYNC";
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) {
      return Action.Complete;
    }

  };

  public static class Adapter implements
      ParameterDescriptions, RowDescription, NoData, DataRow, PortalSuspended,
      BindComplete, CloseComplete, CommandComplete, ParseComplete,
      FunctionResult, EmptyQuery, ReportNotice, CommandError, ReadyForQuery {

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) {
      return Action.Resume;
    }

    @Override
    public Action parameterDescriptions(TypeRef[] types) {
      return Action.Resume;
    }

    @Override
    public Action rowDescription(ResultField[] fields) {
      return Action.Resume;
    }

    @Override
    public Action rowData(ByteBuf data) {
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() {
      return Action.Resume;
    }

    @Override
    public Action noData() {
      return Action.Resume;
    }

    @Override
    public Action parseComplete() {
      return Action.Resume;
    }

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action closeComplete() {
      return Action.Resume;
    }

    @Override
    public Action emptyQuery() {
      return Action.Resume;
    }

    @Override
    public Action functionResult(ByteBuf data) {
      return Action.Resume;
    }

    @Override
    public Action error(Notice notice) {
      return Action.Resume;
    }

    @Override
    public Action notice(Notice notice) {
      return Action.Resume;
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) {
      return Action.Resume;
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

  public static class DefaultHander extends Adapter {

  }

}
