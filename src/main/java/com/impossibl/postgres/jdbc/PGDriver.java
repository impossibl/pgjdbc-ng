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

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Version;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Driver implementation
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGDriver implements Driver {
  /** The version of the driver */
  public static final Version VERSION = Version.get(0, 1, 0);

  /** The housekeeper */
  private ThreadedHousekeeper realHousekeeper = new ThreadedHousekeeper();

  public PGDriver() throws SQLException {
    DriverManager.registerDriver(this);
  }

  @Override
  public PGConnection connect(String url, Properties info) throws SQLException {
    return ConnectionUtil.createConnection(url, info, realHousekeeper);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return ConnectionUtil.parseURL(url) != null;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    List<DriverPropertyInfo> propInfo = new ArrayList<>();

    ConnectionUtil.ConnectionSpecifier spec = ConnectionUtil.parseURL(url);
    if (spec == null)
      spec = new ConnectionUtil.ConnectionSpecifier();

    if (spec.getDatabase() == null || spec.getDatabase().isEmpty())
      propInfo.add(new DriverPropertyInfo("database", ""));

    if (spec.getParameters().get("username") == null || spec.getParameters().get("username").toString().isEmpty())
      propInfo.add(new DriverPropertyInfo("username", ""));

    if (spec.getParameters().get("password") == null || spec.getParameters().get("password").toString().isEmpty())
      propInfo.add(new DriverPropertyInfo("password", ""));

    return propInfo.toArray(new DriverPropertyInfo[propInfo.size()]);
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(Context.class.getPackage().getName());
  }
}
