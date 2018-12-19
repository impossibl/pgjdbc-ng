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
import com.impossibl.postgres.protocol.RequestExecutor.PrepareHandler;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NoData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterDescriptions;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.RowDescription;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.TypeRef;

import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PrepareRequest implements ServerRequest {

  private String query;
  private String statementName;
  private Type[] suggestedParameterTypes;
  private PrepareHandler handler;
  private TypeRef[] describedParameterTypes;
  private List<Notice> notices;

  PrepareRequest(String statementName, String query, Type[] suggestedParameterTypes, PrepareHandler handler) {
    this.statementName = statementName;
    this.query = query;
    this.suggestedParameterTypes = suggestedParameterTypes;
    this.handler = handler;
    this.notices = new ArrayList<>();
  }

  private class Handler implements ParameterDescriptions, RowDescription, NoData, ReportNotice, ParseComplete, CommandError {

    @Override
    public String toString() {
      return "Prepare";
    }

    @Override
    public Action parseComplete() {
      return Action.Resume;
    }

    @Override
    public Action parameterDescriptions(TypeRef[] types) {
      describedParameterTypes = types;
      return Action.Resume;
    }

    @Override
    public Action rowDescription(ResultField[] fields) throws IOException {

      handler.handleComplete(describedParameterTypes, fields, notices);

      return Action.Sync;
    }

    @Override
    public Action noData() throws IOException {

      handler.handleComplete(describedParameterTypes, EMPTY_FIELDS, notices);

      return Action.Sync;
    }

    @Override
    public Action notice(Notice notice) {

      notices.add(notice);

      return Action.Resume;
    }

    @Override
    public Action error(Notice error) throws IOException {

      handler.handleError(new NoticeException(error), notices);

      return Action.Sync;
    }

    @Override
    public void exception(Throwable cause) throws IOException {

      handler.handleError(cause, notices);

    }

  }

  @Override
  public ProtocolHandler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel
        .writeParse(statementName, query, suggestedParameterTypes)
        .writeDescribe(Statement, statementName)
        .writeSync()
        .flush();

  }

}
