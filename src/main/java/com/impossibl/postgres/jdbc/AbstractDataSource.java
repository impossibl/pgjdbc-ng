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
import com.impossibl.postgres.system.Settings;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static java.lang.Boolean.parseBoolean;

import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
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
  private int parsedSqlCacheSize;
  private int preparedStatementCacheSize;
  private String applicationName;
  private String clientEncoding;
  private int networkTimeout;
  private boolean strictMode;
  private int defaultFetchSize;

  private boolean ssl;
  private String sslMode;
  private String sslPassword;
  private String sslCertificateFile;
  private String sslKeyFile;
  private String sslRootCertificateFile;

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
    this.housekeeper = parseBoolean(PGSettings.HOUSEKEEPER_DEFAULT_DATASOURCE);
    this.parsedSqlCacheSize = Settings.PARSED_SQL_CACHE_SIZE_DEFAULT;
    this.preparedStatementCacheSize = Settings.PREPARED_STATEMENT_CACHE_SIZE_DEFAULT;
    this.applicationName = null;
    this.clientEncoding = null;
    this.networkTimeout = Settings.NETWORK_TIMEOUT_DEFAULT;
    this.strictMode = Settings.STRICT_MODE_DEFAULT;
    this.defaultFetchSize = Settings.DEFAULT_FETCH_SIZE_DEFAULT;

    this.ssl = false;
    this.sslMode = null;
    this.sslPassword = null;
    this.sslCertificateFile = null;
    this.sslKeyFile = null;
    this.sslRootCertificateFile = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    loginTimeout = seconds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    // Not supported
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // Not supported
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(Context.class.getPackage().getName());
  }

  /**
   * Create a reference using the correct ObjectFactory instance
   * @return The reference
   */
  protected abstract Reference createReference();

  /**
   * {@inheritDoc}
   */
  public Reference getReference() throws NamingException {
    Reference ref = createReference();

    if (host != null)
      ref.add(new StringRefAddr("host", host));

    if (port != 5432)
      ref.add(new StringRefAddr("port", Integer.toString(port)));

    if (database != null)
      ref.add(new StringRefAddr("database", database));

    if (user != null)
      ref.add(new StringRefAddr("user", user));

    if (password != null)
      ref.add(new StringRefAddr("password", password));

    if (housekeeper != parseBoolean(PGSettings.HOUSEKEEPER_DEFAULT_DATASOURCE))
      ref.add(new StringRefAddr("housekeeper", Boolean.toString(housekeeper)));

    if (parsedSqlCacheSize != Settings.PARSED_SQL_CACHE_SIZE_DEFAULT)
      ref.add(new StringRefAddr("parsedSqlCacheSize", Integer.toString(parsedSqlCacheSize)));

    if (preparedStatementCacheSize != Settings.PREPARED_STATEMENT_CACHE_SIZE_DEFAULT)
      ref.add(new StringRefAddr("preparedStatementCacheSize", Integer.toString(preparedStatementCacheSize)));

    if (applicationName != null)
      ref.add(new StringRefAddr("applicationName", applicationName));

    if (clientEncoding != null)
      ref.add(new StringRefAddr("clientEncoding", clientEncoding));

    if (networkTimeout != 0)
      ref.add(new StringRefAddr("networkTimeout", Integer.toString(networkTimeout)));

    if (strictMode != Settings.STRICT_MODE_DEFAULT)
      ref.add(new StringRefAddr("strictMode", Boolean.toString(strictMode)));

    if (defaultFetchSize != Settings.DEFAULT_FETCH_SIZE_DEFAULT)
      ref.add(new StringRefAddr("defaultFetchSize", Integer.toString(defaultFetchSize)));

    if (ssl) {
      ref.add(new StringRefAddr("ssl", "true"));

      if (sslMode != null)
        ref.add(new StringRefAddr("sslMode", sslMode));

      if (sslPassword != null)
        ref.add(new StringRefAddr("sslPassword", sslPassword));

      if (sslCertificateFile != null)
        ref.add(new StringRefAddr("sslCertificateFile", sslCertificateFile));

      if (sslKeyFile != null)
        ref.add(new StringRefAddr("sslKeyFile", sslKeyFile));

      if (sslRootCertificateFile != null)
        ref.add(new StringRefAddr("sslRootCertificateFile", sslRootCertificateFile));
    }

    return ref;
  }

  /**
   * Init
   * @param reference The reference
   */
  public void init(Reference reference) {
    String value = null;

    value = getReferenceValue(reference, "host");
    if (value != null)
      host = value;

    value = getReferenceValue(reference, "port");
    if (value != null)
      port = Integer.valueOf(value);

    value = getReferenceValue(reference, "database");
    if (value != null)
      database = value;

    value = getReferenceValue(reference, "user");
    if (value != null)
      user = value;

    value = getReferenceValue(reference, "password");
    if (value != null)
      password = value;

    value = getReferenceValue(reference, "housekeeper");
    if (value != null)
      housekeeper = Boolean.valueOf(value);

    value = getReferenceValue(reference, "parsedSqlCacheSize");
    if (value != null)
       parsedSqlCacheSize = Integer.valueOf(value);

    value = getReferenceValue(reference, "preparedStatementCacheSize");
    if (value != null)
       preparedStatementCacheSize = Integer.valueOf(value);

    value = getReferenceValue(reference, "applicationName");
    if (value != null)
      applicationName = value;

    value = getReferenceValue(reference, "clientEncoding");
    if (value != null)
      clientEncoding = value;

    value = getReferenceValue(reference, "networkTimeout");
    if (value != null)
       networkTimeout = Integer.valueOf(value);

    value = getReferenceValue(reference, "strictMode");
    if (value != null)
      strictMode = Boolean.valueOf(value);

    value = getReferenceValue(reference, "defaultFetchSize");
    if (value != null)
      defaultFetchSize = Integer.valueOf(value);

    value = getReferenceValue(reference, "ssl");
    if (value != null)
      ssl = true;

    if (ssl) {
      value = getReferenceValue(reference, "sslMode");
      if (value != null)
        sslMode = value;

      value = getReferenceValue(reference, "sslPassword");
      if (value != null)
        sslPassword = value;

      value = getReferenceValue(reference, "sslCertificateFile");
      if (value != null)
        sslCertificateFile = value;

      value = getReferenceValue(reference, "sslKeyFile");
      if (value != null)
        sslKeyFile = value;

      value = getReferenceValue(reference, "sslRootCertificateFile");
      if (value != null)
        sslRootCertificateFile = value;
    }
  }

  /**
   * Get reference value
   * @param reference The reference
   * @param key The key
   * @return The value
   */
  private String getReferenceValue(Reference reference, String key) {
    RefAddr refAddr = reference.get(key);

    if (refAddr == null)
      return null;

    return (String)refAddr.getContent();
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
   * Get the size of the parsed SQL cache.
   * @return the number of SQL statements' parsed structures allowed in the cache
   */
  public int getParsedSqlCacheSize() {
    return parsedSqlCacheSize;
  }

  /**
   * Set the size of the parsed SQL cache.  A value of 0 will disable
   * the cache.  This value is only honored before the creation of the
   * first Connection, changing it at a later time will have no effect.
   * @param cacheSize the number of SQL statements' parsed structures to cache
   */
  public void setParsedSqlCacheSize(int cacheSize) {
    parsedSqlCacheSize = cacheSize;
  }

  /**
   * Get the size of the prepared statement cache
   *
   * @return the maximum number of PreparedStatements cached per connection
   */
  public int getPreparedStatementCacheSize() {
    return preparedStatementCacheSize;
  }

  /**
   * Set the size of the preapred statement cache
   *
   * @param preparedStatementCacheSize
   *          the maximum number of PreparedStatements cached per connection
   */
  public void setPreparedStatementCacheSize(int preparedStatementCacheSize) {
    this.preparedStatementCacheSize = preparedStatementCacheSize;
  }

  /**
   * Get the application name
   * @return The value
   */
  public String getApplicationName() {
    return applicationName;
  }

  /**
   * Set the application name
   * @param v The value
   */
  public void setApplicationName(String v) {
    applicationName = v;
  }

  /**
   * Get the client encoding
   * @return The value
   */
  public String getClientEncoding() {
    return clientEncoding;
  }

  /**
   * Set the client encoding
   * @param v The value
   */
  public void setClientEncoding(String v) {
    clientEncoding = v;
  }

  /**
   * Set the network timeout in milliseconds
   * @param networkTimeout The value
   */
  public void setNetworkTimeout(int networkTimeout) {
    this.networkTimeout = networkTimeout;
  }

  /**
   * Get the network timeout in milliseconds
   * @return The value
   */
  public int getNetworkTimeout() {
    return networkTimeout;
  }

  /**
   * Get the strict mode
   * @return The value
   */
  public boolean getStrictMode() {
    return strictMode;
  }

  /**
   * Set the strict mode
   * @param v The value
   */
  public void setStrictMode(boolean v) {
    strictMode = v;
  }

  /**
   * Get the default fetch size
   * @return The value
   */
  public int getDefaultFetchSize() {
    return defaultFetchSize;
  }

  /**
   * Set the default fetch size
   * @param v The value
   */
  public void setDefaultFetchSize(int v) {
    defaultFetchSize = v;
  }

  /**
   * Is SSL enabled
   * @return The value
   */
  public boolean getSSL() {
    return ssl;
  }

  /**
   * Enable/disable SSL
   * @param v The value
   */
  public void setSSL(boolean v) {
    ssl = v;
  }

  /**
   * Get the SSL mode
   * @return The value
   */
  public String getSSLMode() {
    return sslMode;
  }

  /**
   * Set the SSL mode
   * @param v The value
   */
  public void setSSLMode(String v) {
    sslMode = v;
  }

  /**
   * Get the SSL password
   * @return The value
   */
  public String getSSLPassword() {
    return sslPassword;
  }

  /**
   * Set the SSL password
   * @param v The value
   */
  public void setSSLPassword(String v) {
    sslPassword = v;
  }

  /**
   * Get the SSL certificate file
   * @return The value
   */
  public String getSSLCertificateFile() {
    return sslCertificateFile;
  }

  /**
   * Set the SSL certificate file
   * @param v The value
   */
  public void setSSLCertificateFile(String v) {
    sslCertificateFile = v;
  }

  /**
   * Get the SSL key file
   * @return The value
   */
  public String getSSLKeyFile() {
    return sslKeyFile;
  }

  /**
   * Set the SSL key file
   * @param v The value
   */
  public void setSSLKeyFile(String v) {
    sslKeyFile = v;
  }

  /**
   * Get the SSL root certificate file
   * @return The value
   */
  public String getSSLRootCertificateFile() {
    return sslRootCertificateFile;
  }

  /**
   * Set the SSL root certificate file
   * @param v The value
   */
  public void setSSLRootCertificateFile(String v) {
    sslRootCertificateFile = v;
  }

  /**
   * Create a connection
   *
   * @param u
   *          The user name
   * @param p
   *          The password
   * @return The connection
   * @exception SQLException
   *              Thrown in case of an error
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

    props.put(Settings.PARSED_SQL_CACHE_SIZE, parsedSqlCacheSize);
    props.put(Settings.PREPARED_STATEMENT_CACHE_SIZE, preparedStatementCacheSize);
    if (applicationName != null)
      props.put(Settings.APPLICATION_NAME, applicationName);
    if (clientEncoding != null)
      props.put(Settings.CLIENT_ENCODING, clientEncoding);
    props.put(Settings.NETWORK_TIMEOUT, Integer.toString(networkTimeout));
    props.put(Settings.STRICT_MODE, Boolean.toString(strictMode));
    props.put(Settings.DEFAULT_FETCH_SIZE, Integer.toString(defaultFetchSize));

    if (ssl) {
      if (sslMode != null)
        props.put(Settings.SSL_MODE, sslMode);
      else
        props.put(Settings.SSL_MODE, "Require");

      if (sslPassword != null)
        props.put(Settings.SSL_PASSWORD, sslPassword);

      if (sslCertificateFile != null)
        props.put(Settings.SSL_CERT_FILE, sslCertificateFile);

      if (sslKeyFile != null)
        props.put(Settings.SSL_KEY_FILE, sslKeyFile);

      if (sslRootCertificateFile != null)
        props.put(Settings.SSL_ROOT_CERT_FILE, sslRootCertificateFile);
    }

    return ConnectionUtil.createConnection(url, props, housekeeper);
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
