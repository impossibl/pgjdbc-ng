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

import com.impossibl.postgres.protocol.v30.ServerConnectionShared;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.types.SharedRegistry;

import static com.impossibl.postgres.jdbc.JDBCSettings.JDBC;
import static com.impossibl.postgres.jdbc.JDBCSettings.REGISTRY_SHARING;
import static com.impossibl.postgres.system.SystemSettings.PROTO;
import static com.impossibl.postgres.system.SystemSettings.SYS;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverAction;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Driver implementation
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGDriver implements Driver, DriverAction {

  private static final Properties DRIVER = new Properties();
  static {
    try {
      DRIVER.load(PGDriver.class.getClassLoader().getResourceAsStream("META-INF/pgjdbc-ng.properties"));
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /** The name of the driver */
  public static final String NAME;
  static {
    NAME = DRIVER.getProperty("name");
  }

  /** The version of the driver */
  public static final Version VERSION;
  static {
    VERSION = Version.parse(DRIVER.getProperty("version"));
  }

  public static final Logger logger = Logger.getLogger(PGDriver.class.getName());

  private static PGDriver registered;

  static {

    try {
      registered = new PGDriver();
      DriverManager.registerDriver(registered);
    }
    catch (SQLException e) {
      logger.log(Level.SEVERE, "Error registering driver", e);
    }

  }

  public PGDriver() {
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {

    SharedRegistry.Factory sharedRegistryFactory = SharedRegistry.getFactory(REGISTRY_SHARING.get(info));

    PGDirectConnection conn = ConnectionUtil.createConnection(url, info, sharedRegistryFactory);
    if (conn == null) return null;

    return APITracing.setupIfEnabled(conn);
  }

  @Override
  public boolean acceptsURL(String url) {
    return ConnectionUtil.parseURL(url) != null;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {

    return Stream.concat(JDBC.getAllSettings().stream(), Stream.concat(SYS.getAllSettings().stream(), PROTO.getAllSettings().stream()))
        .map(setting -> {
          DriverPropertyInfo pi = new DriverPropertyInfo(setting.getName(), setting.getText(info));
          pi.description = setting.getDescription();
          return pi;
        })
        .toArray(DriverPropertyInfo[]::new);
  }

  @Override
  public int getMajorVersion() {
    return VERSION.getMajor();
  }

  @Override
  public int getMinorVersion() {
    return VERSION.getMinor();
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger("com.impossibl.postgres");
  }

  @Override
  public void deregister() {
    cleanup();
  }

  public static void cleanup() {

    if (registered != null) {
      try {
        DriverManager.deregisterDriver(registered);
      }
      catch (SQLException e) {
        logger.log(Level.WARNING, "Error deregistering driver", e);
      }
    }

    ServerConnectionShared.waitForShutdown();
  }

}
