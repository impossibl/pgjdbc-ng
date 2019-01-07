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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.system.Setting;

public class JDBCSettings implements Setting.Provider {

  public static final Setting.Group JDBC = new Setting.Group(
      "jdbc", "JDBC specific settings"
  );

  public static final Setting<Boolean> READ_ONLY = JDBC.add(
      "Connect in read-only mode",
      false,
      "read-only", "readOnly"
  );

  public static final Setting<Integer> PARSED_SQL_CACHE_SIZE = JDBC.add(
      "Size of the parsed SQL text cache (a value less than one disables the cache)",
      250,
      "parsed-sql.cache.size", "parsedSqlCacheSize"
  );

  public static final Setting<Integer> PREPARED_STATEMENT_CACHE_SIZE = JDBC.add(
      "Size of the prepared statement cache (a value less than one disables the cache)",
      50,
      "prepared-statement.cache.size", "preparedStatementCacheSize"
  );

  public static final Setting<Integer> PREPARED_STATEMENT_CACHE_THRESHOLD = JDBC.add(
      "# of times a query is seen before it is cached as a prepared statement (a value of zero prepares all statements in advance)",
      0,
      "prepared-statement.cache.threshold", "preparedStatementCacheThreshold"
  );

  public static final Setting<Integer> DESCRIPTION_CACHE_SIZE = JDBC.add(
      "Size of the query description cache (a value less than one disables the cache)",
      250,
      "description.cache.size", "descriptionCacheSize"
  );

  public static final Setting<Integer> DEFAULT_NETWORK_TIMEOUT = JDBC.add(
      "Default network timeout, can be changed at runtime (a value of zero disables the timeout)",
      0,
      "network.timeout", "networkTimeout"
  );

  public static final Setting<Boolean> STRICT_MODE = JDBC.add(
      "Enable or disable strict adherence to specification",
      false,
      "strict-mode", "strictMode"
  );

  public static final Setting<Integer> DEFAULT_FETCH_SIZE = JDBC.add(
      "Default fetch size of query results, can be changed at runtime (a value of zero disables batching result)",
      (Integer) null,
      "fetch.size", "fetchSize"
  );

  public static final Setting<Boolean> HOUSEKEEPER = JDBC.add(
      "Enables or disables the JDBC housekeeping system for leaked API objects",
      true,
      "housekeeper", "housekeeper.enabled"
  );

  public static final Setting<Boolean> REGISTRY_SHARING = JDBC.add(
      "Enables or disables registry sharing",
      true,
      "registry.sharing", "registrySharing"
  );

}
