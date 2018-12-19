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
package com.impossibl.postgres.protocol;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.TypeRef;

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface RequestExecutor {

  interface ErrorHandler {

    void handleError(Throwable cause, List<Notice> notices) throws IOException;

  }

  /*****
   * Query requests. Directly execute unparsed SQL text.
   */


  interface QueryHandler extends ErrorHandler {

    void handleComplete(String command, Long rowsAffected, Long insertedOid, TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) throws IOException;
    void handleSuspend(TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) throws IOException;
    void handleReady() throws IOException;

  }


  void query(String sql, QueryHandler handler) throws IOException;
  void query(String sql, String portalName,
             FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers,
             FieldFormatRef[] resultFieldFormats, int maxRows, QueryHandler handler) throws IOException;


  /*****
   * Prepare. Prepares a named query for execution.
   */


  interface PrepareHandler extends ErrorHandler {

    void handleComplete(TypeRef[] parameterTypes, ResultField[] resultFields, List<Notice> notices) throws IOException;

  }

  void prepare(String statementName, String sqlText, Type[] parameterTypes, PrepareHandler handler) throws IOException;


  /*****
   * Execute. Execute a previously prepared query & allows resuming suspended queries.
   */


  interface ExecuteHandler extends ErrorHandler {

    void handleComplete(String command, Long rowsAffected, Long insertedOid, RowDataSet rows, List<Notice> notices) throws IOException;
    void handleSuspend(RowDataSet rows, List<Notice> notices) throws IOException;

  }

  void execute(String portalName, String statementName,
               FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers,
               FieldFormatRef[] resultFieldFormatRefs, int maxRows,
               ExecuteHandler handler) throws IOException;

  void resume(String portalName, int maxRows, ExecuteHandler handler) throws IOException;


  /*****
   * Function Call
   */


  interface FunctionCallHandler extends ErrorHandler {

    void handleComplete(ByteBuf result, List<Notice> notices);

  }

  void call(int functionId,
                     FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers,
                     FunctionCallHandler handler) throws IOException;


  /*****
   * Utility requests. Generally these are "fire and forget" with no completion handlers.
   */


  void close(ServerObjectType objectType, String objectName) throws IOException;
  void lazyExecute(String statementName) throws IOException;


  /*****
   * Asynchronous Notification
   */

  interface NotificationHandler {

    void handleNotification(int processId, String channelName, String payload) throws IOException;

  }

}
