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

import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Version;

import java.io.IOException;


/**
 * A system table object.
 *
 * Facilitates easy exploitation of the auto-mapping feature of the driver.
 * Providing a POJO type for the row and some matching SQL and a list of
 * instances can be fetched from the database with no other work.  This is
 * used for system queries.
 *
 * @author kdubb
 *
 * @param <R> The row type this table uses
 */
public interface Table<R extends Table.Row> {

  /**
   * Returns the SQL that is needed to load all rows of this table.
   *
   * @param currentVersion Current version of the server that will execute it
   * @return SQL text of the query
   */
  String getSQL(Version currentVersion);

  /**
   * Creates and instance of the row type of this table.
   *
   * @return An instance of the table's row type
   */
  R createRow(Context context, RowData rowData) throws IOException;


  interface Row {

    void load(Context context, RowData rowData) throws IOException;

  }

}
