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
import com.impossibl.postgres.system.SystemSettings;

@Setting.Factory
public class DataSourceSettings {

  @Setting.Group.Info(
      id = "jdbc-ds", desc = "JDBC DataSource Settings", order = 2
  )
  public static final Setting.Group DS = Setting.Group.declare();

  @Setting.Info(
      desc = "Name of data source.",
      name = "data-source.name",
      group = "jdbc-ds",
      alternateNames = {"dataSourceName"}
  )
  public static final Setting<String> DATASOURCE_NAME = Setting.declare();

  public static final Setting<String> DATABASE_NAME = DS.add(
      SystemSettings.DATABASE_NAME
  );

  @Setting.Info(
      desc = "Host name for TCP connections.",
      def = "localhost",
      name = "server.name",
      group = "jdbc-ds",
      alternateNames = {"serverName"}
  )
  public static final Setting<String> SERVER_NAME = Setting.declare();

  @Setting.Info(
      desc = "Port number for TCP connections.",
      def = "5432", min = 1, max = 65535,
      name = "port.number",
      group = "jdbc-ds",
      alternateNames = {"portNumber"}
  )
  public static final Setting<Integer> PORT_NUMBER = Setting.declare();

  @Setting.Info(
      desc = "Maximum time to wait for a connection to be established.",
      def = "0", min = 0,
      name = "login.timeout",
      group = "jdbc-ds",
      alternateNames = {"loginTimeout"}
  )
  public static final Setting<Integer> LOGIN_TIMEOUT = Setting.declare();

  static {
    DataSourceSettingsInit.init();
  }

}
