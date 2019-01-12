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

import com.impossibl.jdbc.spy.ConnectionRelay;
import com.impossibl.jdbc.spy.ConnectionTracer;
import com.impossibl.jdbc.spy.SimpleTraceOutput;
import com.impossibl.postgres.system.Configuration;
import com.impossibl.postgres.system.Settings;

import static com.impossibl.postgres.jdbc.JDBCSettings.API_TRACE;
import static com.impossibl.postgres.jdbc.JDBCSettings.API_TRACE_FILE;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class APITracing {

  private static final Logger logger = Logger.getLogger(APITracing.class.getName());

  public static Connection setupIfEnabled(PGDirectConnection connection) {
    return setupIfEnabled(connection, connection);
  }

  public static Connection setupIfEnabled(Connection connection, Configuration config) {
    if (!config.getSetting(API_TRACE)) return connection;
    return setup(connection, config.getSetting(API_TRACE_FILE));
  }

  public static Connection setupIfEnabled(Connection connection, Settings settings) {
    if (!settings.get(API_TRACE)) return connection;
    return setup(connection, settings.get(API_TRACE_FILE));
  }

  public static ConnectionRelay setup(Connection connection, String file) {
    OutputStream out = System.out;

    if (file != null) {
      try {
        out = new FileOutputStream(file);
      }
      catch (IOException e) {
        logger.log(Level.WARNING, "Unable to initialize API tracing", e);
        return null;
      }
    }
    return new ConnectionRelay(connection, new ConnectionTracer(new SimpleTraceOutput(new OutputStreamWriter(out))));
  }

}
