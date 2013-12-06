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

import com.impossibl.postgres.system.Context;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.CommonDataSource;

/**
 * Abstract DataSource implementation
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public abstract class AbstractDataSource implements CommonDataSource {
  private int loginTimeout;

  private String host;
  private int port;
  private String database;
  private String user;
  private String password;
  private boolean housekeeper;

  /**
   * Constructor
   */
  protected AbstractDataSource() {
    this.loginTimeout = 0;
    this.host = "localhost";
    this.port = 5432;
    this.database = null;
    this.user = null;
    this.password = null;
    this.housekeeper = true;
  }

  /**
   * {@inheritDoc}
   */
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  /**
   * {@inheritDoc}
   */
  public void setLoginTimeout(int seconds) throws SQLException {
    loginTimeout = seconds;
  }

  /**
   * {@inheritDoc}
   */
  public PrintWriter getLogWriter() throws SQLException {
    // Not supported
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public void setLogWriter(PrintWriter out) throws SQLException {
    // Not supported
  }

  /**
   * {@inheritDoc}
   */
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(Context.class.getPackage().getName());
  }

  /**
   * Get the host
   * @return The value
   */
  public String getHost() {
    return host;
  }

  /**
   * Set the host
   * @param v The value
   */
  public void setHost(String v) {
    host = v;
  }

  /**
   * Get the port
   * @return The value
   */
  public int getPort() {
    return port;
  }

  /**
   * Set the port
   * @param v The value
   */
  public void setPort(int v) {
    port = v;
  }

  /**
   * Get the database
   * @return The value
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Set the database
   * @param v The value
   */
  public void setDatabase(String v) {
    database = v;
  }

  /**
   * Get the user
   * @return The value
   */
  public String getUser() {
    return user;
  }

  /**
   * Set the user
   * @param v The value
   */
  public void setUser(String v) {
    user = v;
  }

  /**
   * Get the password
   * @return The value
   */
  public String getPassword() {
    return password;
  }

  /**
   * Set the password
   * @param v The value
   */
  public void setPassword(String v) {
    password = v;
  }

  /**
   * Get the housekeeper
   * @return The value
   */
  public boolean getHousekeeper() {
    return housekeeper;
  }

  /**
   * Set the housekeeper
   * @param v The value
   */
  public void setHousekeeper(boolean v) {
    housekeeper = v;
  }

  /**
   * Create a connection
   * @param u The user name
   * @param p The password
   * @return The connection
   * @exception SQLException Thrown in case of an error
   */
  protected PGConnectionImpl createConnection(String u, String p) throws SQLException {
    String url = buildUrl();
    Properties props = new Properties();

    if (u != null) {
      props.put("user", u);
    }
    else if (user != null) {
      props.put("user", user);
    }
    else {
      props.put("user", "");
    }

    if (p != null) {
      props.put("password", p);
    }
    else if (password != null) {
      props.put("password", password);
    }
    else {
      props.put("password", "");
    }

    Housekeeper hk = null;
    if (housekeeper)
      hk = new ThreadedHousekeeper();

    return ConnectionUtil.createConnection(url, props, hk);
  }

  private String buildUrl() throws SQLException {
    StringBuilder sb = new StringBuilder();

    if (getDatabase() == null)
       throw new SQLException("Database parameter mandatory for " + getHost() + ":" + getPort());

    sb = sb.append("jdbc:pgsql://");
    sb = sb.append(getHost());
    sb = sb.append(":");
    sb = sb.append(getPort());
    sb = sb.append("/");
    sb = sb.append(getDatabase());

    return sb.toString();
  }
}
