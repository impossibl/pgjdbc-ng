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
import com.impossibl.postgres.system.UnsupportedServerVersion;
import com.impossibl.postgres.system.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Tables {

  /**
   * Helper function for matching a SQL query with a version.
   *
   * @param sqlData An array of [Version, String] pairs to select from
   * @param currentVersion The requested version of SQL to retrieve
   * @return SQL text for the requested version
   * @throws UnsupportedServerVersion if no match can be found
   * @throws IllegalStateException if the sqlData pairs are ill formed
   */
  public static String getSQL(Object[] sqlData, Version currentVersion) {

    try {

      for (int c = 0; c < sqlData.length; c += 2) {

        Version curSqlVersion = (Version) sqlData[c];
        String curSql = (String) sqlData[c + 1];

        if (currentVersion.isMinimum(curSqlVersion))
          return curSql;
      }

    }
    catch (Exception e) {
      throw new IllegalStateException("Misconfigured system table type ");
    }

    throw new UnsupportedServerVersion(currentVersion);
  }

  public static <R extends Table.Row, T extends Table<R>> List<R> convertRows(Context context, T table, ResultBatch results) throws IOException {
    List<R> rows = new ArrayList<>(results.getResults().size());
    for (ResultBatch.Row row : results) {
      rows.add(table.createRow(context, row));
    }
    return rows;
  }

}
