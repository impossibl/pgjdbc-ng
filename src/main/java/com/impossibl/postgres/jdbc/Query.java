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

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;

import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface Query {

  enum Status {
    Initialized,
    InProgress,
    Completed,
    Suspended,
  }

  Status getStatus();

  Long getTimeout();
  void setTimeout(Long timeout);

  void setMaxRows(Integer maxRows);

  List<ResultBatch> getResultBatches();

  SQLWarning execute(PGDirectConnection connection) throws SQLException;

  void dispose(PGDirectConnection connection) throws SQLException;

  static Query create(String sqlText) {
    return new DirectQuery(sqlText, EMPTY_FORMATS, EMPTY_BUFFERS, EMPTY_FORMATS);
  }

  static Query create(String sqlText, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers) {
    if (parameterFormats == null && parameterBuffers == null) {
      return create(sqlText);
    }
    return new DirectQuery(sqlText, parameterFormats, parameterBuffers, EMPTY_FORMATS);
  }

  static Query create(String statement, ResultField[] resultFields) {
    return new PreparedQuery(statement, EMPTY_FORMATS, EMPTY_BUFFERS, resultFields);
  }

  static Query create(String statement, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, ResultField[] resultFields) {
    return new PreparedQuery(statement, parameterFormats, parameterBuffers, resultFields);
  }

}
