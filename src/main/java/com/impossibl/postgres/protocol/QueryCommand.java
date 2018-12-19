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

import java.util.ArrayList;
import java.util.List;

public interface QueryCommand extends Command {

  enum Status {
    Completed,
    Suspended
  }

  class ResultBatch {
    private String command;
    private Long rowsAffected;
    private Long insertedOid;
    private ResultField[] fields;
    private List<RowData> results;

    public ResultBatch() {
      command = null;
      rowsAffected = null;
      insertedOid = null;
      fields = null;
      results = null;
    }

    public void setCommand(String v) {
      command = v;
    }

    public String getCommand() {
      return command;
    }

    public void setRowsAffected(Long v) {
      rowsAffected = v;
    }

    public Long getRowsAffected() {
      return rowsAffected;
    }

    public void setInsertedOid(Long v) {
      insertedOid = v;
    }

    public Long getInsertedOid() {
      return insertedOid;
    }

    public void setFields(ResultField[] v) {
      fields = v;
    }

    public ResultField[] getFields() {
      return fields;
    }

    public void addResult(RowData v) {
      if (results == null)
        results = new ArrayList<>();

      results.add(v);
    }

    public void resetResults(boolean allowEmpty) {
      if (results != null) {
        for (RowData dataRow : results) {
          dataRow.release();
        }
      }

      results = (allowEmpty && fields != null) ? new ArrayList<>() : null;
    }

    public List<RowData> getResults() {
      return results;
    }

    public void release() {
      resetResults(false);
    }

    public void touch(Object hint) {
      if (results != null) {
        for (RowData row : results) {
          row.touch(hint);
        }
      }
    }

    public static List<ResultBatch> releaseResultBatches(List<ResultBatch> resultBatches) {
      if (resultBatches != null) {
        for (ResultBatch resultBatch : resultBatches) {
          resultBatch.release();
        }
      }
      return null;
    }
  }

  void setRequireActiveTransaction(boolean requireActiveTransaction);

  void setQueryTimeout(long timeout);

  void setMaxRows(int maxRows);

  List<ResultBatch> getResultBatches();

  Status getStatus();

}
