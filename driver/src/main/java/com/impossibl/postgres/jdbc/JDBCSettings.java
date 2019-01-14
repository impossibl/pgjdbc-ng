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

import com.impossibl.postgres.system.ParameterNames;
import com.impossibl.postgres.system.Setting;

@Setting.Factory
public class JDBCSettings {

  @Setting.Group.Info(
      id = "jdbc", desc = "JDBC Settings", order = 1
  )
  public static final Setting.Group JDBC = Setting.Group.declare();

  @Setting.Info(
      desc = "Connect in read-only mode",
      def = "false",
      name = "read-only",
      group = "jdbc",
      alternateNames = "readOnly"
  )
  public static final Setting<Boolean> READ_ONLY = Setting.declare();

  @Setting.Info(
      desc = "Size of the parsed SQL text cache.\n\nA value of zero disables the cache.",
      def = "250", min = 0,
      name = "parsed-sql.cache.size",
      group = "jdbc",
      alternateNames = "parsedSqlCacheSize"
  )
  public static final Setting<Integer> PARSED_SQL_CACHE_SIZE = Setting.declare();

  @Setting.Info(
      desc = "Size of the prepared statement cache\n\nA value of zero disables the cache.",
      def = "50", min = 0,
      name = "prepared-statement.cache.size",
      group = "jdbc",
      alternateNames = "preparedStatementCacheSize"
  )
  public static final Setting<Integer> PREPARED_STATEMENT_CACHE_SIZE = Setting.declare();

  @Setting.Info(
      desc = "# of times a query is seen before it is cached as a prepared statement.\n\nA value of zero prepares all statements in advance.",
      def = "0", min = 0,
      name = "prepared-statement.cache.threshold",
      group = "jdbc",
      alternateNames = "preparedStatementCacheThreshold"
  )
  public static final Setting<Integer> PREPARED_STATEMENT_CACHE_THRESHOLD = Setting.declare();

  @Setting.Info(
      desc = "Size of the query description cache.\n\nA value of zero disables the cache.",
      def = "250", min = 0,
      name = "description.cache.size",
      group = "jdbc",
      alternateNames = "descriptionCacheSize"
  )
  public static final Setting<Integer> DESCRIPTION_CACHE_SIZE = Setting.declare();

  @Setting.Info(
      desc = "Default timeout for network communication.\n\nValue can be changed at runtime through API.\n\nValue of zero disables the timeout.",
      def = "0", min = 0,
      name = "network.timeout",
      group = "jdbc",
      alternateNames = "networkTimeout"
  )
  public static final Setting<Integer> DEFAULT_NETWORK_TIMEOUT = Setting.declare();

  @Setting.Info(
      desc = "Enable or disable strict adherence to JDBC specification.",
      def = "false",
      name = "strict-mode",
      group = "jdbc",
      alternateNames = "strictMode"
  )
  public static final Setting<Boolean> STRICT_MODE = Setting.declare();

  @Setting.Info(
      desc = "Default fetch size of query results.\n\n Value can be changed at runtime.\n\nA value of zero disables batching results.",
      min = 0,
      name = "fetch.size",
      group = "jdbc",
      alternateNames = "fetchSize"
  )
  public static final Setting<Integer> DEFAULT_FETCH_SIZE = Setting.declare();

  @Setting.Info(
      desc = "Enables or disables the housekeeping system for leaked JDBC objects.",
      def = "true",
      name = "housekeeper",
      group = "jdbc",
      alternateNames = "housekeeper.enabled"
  )
  public static final Setting<Boolean> HOUSEKEEPER = Setting.declare();

  @Setting.Info(
      desc = "Enables or disables sharing type registries between connections.",
      def = "true",
      name = "registry.sharing",
      group = "jdbc",
      alternateNames = "registrySharing"
  )
  public static final Setting<Boolean> REGISTRY_SHARING = Setting.declare();

  @Setting.Info(
      desc = "Enables or disables API trace output.\n\n" +
          "<i>NOTE<i>: Currently this is only available with connections vended from <code>DriverManager</code> or" +
          "<code>Driver</code>.",
      def = "false",
      name = "api.trace",
      group = "jdbc"
  )
  public static final Setting<Boolean> API_TRACE = Setting.declare();

  @Setting.Info(
      desc =
          "File destination of API trace output.\n\n" +
              "<b><i>NOTE</i></b> `api.trace` must be `true` to generate trace output",
      name = "api.trace.file",
      group = "jdbc"
  )
  public static final Setting<String> API_TRACE_FILE = Setting.declare();

  @Setting.Group.Info(
      id = "jdbc-client-info", desc = "Settings allowed to be referenced as connection client-info", global = false
  )
  public static final Setting.Group CLIENT_INFO = Setting.Group.declare();

  @Setting.Info(
      desc = "The name of the application currently utilizing the connection",
      name = "ApplicationName",
      group = "jdbc-client-info",
      alternateNames = ParameterNames.APPLICATION_NAME
  )
  public static final Setting<String> CI_APPLICATION_NAME = Setting.declare();

  @Setting.Info(
      desc =
          "The name of the user that the application using the connection is performing work for. This may " +
          "not be the same as the user that was used in establishing the connection.",
      name = "ClientUser",
      group = "jdbc-client-info",
      alternateNames = ParameterNames.SESSION_AUTHORIZATION
  )
  public static final Setting<String> CI_CLIENT_USER = Setting.declare();

  static {
    JDBCSettingsInit.init();
  }

}
