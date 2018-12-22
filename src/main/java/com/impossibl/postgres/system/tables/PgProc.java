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
package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Version;

import static com.impossibl.postgres.system.tables.Table.getFieldOfRow;

import java.io.IOException;


/**
 * Table for "pg_proc"
 *
 * @author kdubb
 *
 */
public class PgProc implements Table<PgProc.Row> {

  public static class Row implements Table.Row {

    private int oid;
    private String name;

    public Row() {
    }

    public void load(Context context, ResultBatch resultBatch, int rowIdx) throws IOException {
      this.oid = getFieldOfRow(resultBatch, rowIdx, OID, context, Integer.class);
      this.name = getFieldOfRow(resultBatch, rowIdx, NAME, context, String.class);
    }

    public int getOid() {
      return oid;
    }

    public void setOid(int v) {
      oid = v;
    }

    public String getName() {
      return name;
    }

    public void setName(String v) {
      name = v;
    }
  }

  static final int OID = 0;
  static final int NAME = 1;

  public static final PgProc INSTANCE = new PgProc();

  private PgProc() {
  }

  @Override
  public String getSQL(Version currentVersion) {
    return Tables.getSQL(SQL, currentVersion);
  }

  @Override
  public Row createRow(Context context, ResultBatch resultBatch, int rowIdx) throws IOException {
    Row row = new Row();
    row.load(context, resultBatch, rowIdx);
    return row;
  }

  private static final Object[] SQL = {Version.get(9, 0, 0), " select " + "    \"oid\", proname as \"name\"" + " from" + "   pg_proc"};

}
