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
package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;
import static com.impossibl.postgres.utils.Nulls.firstNonNull;

import java.util.Arrays;

import io.netty.util.AbstractReferenceCounted;

public class ResultBatch extends AbstractReferenceCounted implements AutoCloseable {

  private String command;
  private Long rowsAffected;
  private Long insertedOid;
  private ResultField[] fields;
  private RowDataSet rows;

  public ResultBatch(String command, Long rowsAffected, Long insertedOid, ResultField[] fields, RowDataSet rows) {
    this.command = command;
    this.rowsAffected = rowsAffected;
    this.insertedOid = insertedOid;
    this.fields = firstNonNull(fields, EMPTY_FIELDS);
    this.rows = rows;
  }

  public boolean hasRows() {
    return fields.length != 0;
  }

  public boolean isEmpty() {
    return !hasRows() || rows.isEmpty();
  }

  public String getCommand() {
    return command;
  }

  public boolean hasRowsAffected() {
    return rowsAffected != null;
  }

  public Long getRowsAffected() {
    if (!hasRowsAffected()) return null;
    return firstNonNull(rowsAffected, 0L);
  }

  public Long getInsertedOid() {
    return insertedOid;
  }

  public ResultField[] getFields() {
    return fields;
  }

  public RowDataSet borrowRows() {
    return rows;
  }

  public RowDataSet takeRows() {
    RowDataSet rows = this.rows;
    this.rows = null;
    this.fields = EMPTY_FIELDS;
    return rows;
  }

  public void clearRowsAffected() {
    this.rowsAffected = null;
  }

  @Override
  protected void deallocate() {
    if (rows != null) {
      rows.release();
    }
  }

  public ResultBatch touch(Object hint) {
    if (rows != null) {
      rows.touch(hint);
    }
    return this;
  }

  @Override
  public void close() {
    release();
  }

  @Override
  public String toString() {
    return "ResultBatch{" +
        "command='" + command + '\'' +
        ", rowsAffected=" + rowsAffected +
        ", insertedOid=" + insertedOid +
        ", fields=" + Arrays.toString(fields) +
        ", rows=" + rows +
        '}';
  }
}
