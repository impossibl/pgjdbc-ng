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
import com.impossibl.postgres.system.ServerConnectionInfo;
import com.impossibl.postgres.types.SharedRegistry;

import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_FETCH_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_NETWORK_TIMEOUT;
import static com.impossibl.postgres.jdbc.JDBCSettings.HOUSEKEEPER;
import static com.impossibl.postgres.jdbc.JDBCSettings.PARSED_SQL_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.PREPARED_STATEMENT_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.REGISTRY_SHARING;
import static com.impossibl.postgres.jdbc.JDBCSettings.STRICT_MODE;
import static com.impossibl.postgres.system.SystemSettings.APPLICATION_NAME;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_ENCODING;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_RECV_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.SSL_CRT_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_FILE_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.SSL_MODE;
import static com.impossibl.postgres.system.SystemSettings.SSL_ROOT_CRT_FILE;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

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

  private String serverName;
  private int portNumber;
  private String databaseName;
  private String user;
  private String password;
  private boolean housekeeper;
  private Integer parsedSqlCacheSize;
  private Integer preparedStatementCacheSize;
  private String applicationName;
  private String clientEncoding;
  private Integer networkTimeout;
  private boolean strictMode;
  private Integer defaultFetchSize;
  private Integer receiveBufferSize;
  private Integer sendBufferSize;

  private boolean ssl;
  private String sslMode;
  private String sslPassword;
  private String sslCertificateFile;
  private String sslKeyFile;
  private String sslRootCertificateFile;

  private Map<ServerConnectionInfo, SharedRegistry> sharedRegistries;

  /**
   * Constructor
   */
  protected AbstractDataSource() {
    this.loginTimeout = 0;
    this.serverName = "localhost";
    this.portNumber = 5432;
    this.databaseName = null;
    this.user = null;
    this.password = null;
    this.housekeeper = HOUSEKEEPER.getDefault();
    this.parsedSqlCacheSize = PARSED_SQL_CACHE_SIZE.getDefault();
    this.preparedStatementCacheSize = PREPARED_STATEMENT_CACHE_SIZE.getDefault();
    this.applicationName = null;
    this.clientEncoding = null;
    this.networkTimeout = DEFAULT_NETWORK_TIMEOUT.getDefault();
    this.strictMode = STRICT_MODE.getDefault();
    this.defaultFetchSize = DEFAULT_FETCH_SIZE.getDefault();
    this.receiveBufferSize = PROTOCOL_SOCKET_RECV_BUFFER_SIZE.getDefault();
    this.sendBufferSize = PROTOCOL_SOCKET_SEND_BUFFER_SIZE.getDefault();

    this.ssl = false;
    this.sslMode = null;
    this.sslPassword = null;
    this.sslCertificateFile = null;
    this.sslKeyFile = null;
    this.sslRootCertificateFile = null;

    this.sharedRegistries = new ConcurrentHashMap<>();
  }

  public abstract String getDescription();

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

    if (serverName != null)
      ref.add(new StringRefAddr("serverName", serverName));

    if (portNumber != 5432)
      ref.add(new StringRefAddr("portNumber", Integer.toString(portNumber)));

    if (databaseName != null)
      ref.add(new StringRefAddr("databaseName", databaseName));

    if (user != null)
      ref.add(new StringRefAddr("user", user));

    if (password != null)
      ref.add(new StringRefAddr("password", password));

    if (housekeeper != HOUSEKEEPER.getDefault())
      ref.add(new StringRefAddr("housekeeper", Boolean.toString(housekeeper)));

    if (parsedSqlCacheSize != PARSED_SQL_CACHE_SIZE.getDefault())
      ref.add(new StringRefAddr("parsedSqlCacheSize", Integer.toString(parsedSqlCacheSize)));

    if (preparedStatementCacheSize != PREPARED_STATEMENT_CACHE_SIZE.getDefault())
      ref.add(new StringRefAddr("preparedStatementCacheSize", Integer.toString(preparedStatementCacheSize)));

    if (applicationName != null)
      ref.add(new StringRefAddr("applicationName", applicationName));

    if (clientEncoding != null)
      ref.add(new StringRefAddr("clientEncoding", clientEncoding));

    if (networkTimeout != 0)
      ref.add(new StringRefAddr("networkTimeout", Integer.toString(networkTimeout)));

    if (strictMode != STRICT_MODE.getDefault())
      ref.add(new StringRefAddr("strictMode", Boolean.toString(strictMode)));

    if (defaultFetchSize != DEFAULT_FETCH_SIZE.getDefault())
      ref.add(new StringRefAddr("defaultFetchSize", Integer.toString(defaultFetchSize)));

    if (receiveBufferSize != PROTOCOL_SOCKET_RECV_BUFFER_SIZE.getDefault())
      ref.add(new StringRefAddr("receiveBufferSize", Integer.toString(receiveBufferSize)));

    if (sendBufferSize != PROTOCOL_SOCKET_SEND_BUFFER_SIZE.getDefault())
      ref.add(new StringRefAddr("sendBufferSize", Integer.toString(sendBufferSize)));

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

    return ref;
  }

  /**
   * Init
   * @param reference The reference
   */
  public void init(Reference reference) {
    String value;

    value = getReferenceValue(reference, "serverName");
    if (value != null)
      serverName = value;

    value = getReferenceValue(reference, "portNumber");
    if (value != null)
      portNumber = Integer.valueOf(value);

    value = getReferenceValue(reference, "databaseName");
    if (value != null)
      databaseName = value;

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

    value = getReferenceValue(reference, "receiveBufferSize");
    if (value != null)
      receiveBufferSize = Integer.valueOf(value);

    value = getReferenceValue(reference, "sendBufferSize");
    if (value != null)
      sendBufferSize = Integer.valueOf(value);

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
  private static String getReferenceValue(Reference reference, String key) {
    RefAddr refAddr = reference.get(key);

    if (refAddr == null)
      return null;

    return (String)refAddr.getContent();
  }

  /**
   * Get the serverName
   * @return The value
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Set the serverName
   * @param v The value
   */
  public void setServerName(String v) {
    serverName = v;
  }

  /**
   * Get the portNumber
   * @return The value
   */
  public int getPortNumber() {
    return portNumber;
  }

  /**
   * Set the portNumber
   * @param v The value
   */
  public void setPortNumber(int v) {
    portNumber = v;
  }

  /**
   * Get the databaseName
   * @return The value
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Set the databaseName
   * @param v The value
   */
  public void setDatabaseName(String v) {
    databaseName = v;
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
   * Get the receive buffer size
   * @return The value
   */
  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  /**
   * Set the receive buffer size
   * @param v The value
   */
  public void setReceiveBufferSize(int v) {
    receiveBufferSize = v;
  }

  /**
   * Get the send buffer size
   * @return The value
   */
  public int getSendBufferSize() {
    return sendBufferSize;
  }

  /**
   * Set the send buffer size
   * @param v The value
   */
  public void setSendBufferSize(int v) {
    sendBufferSize = v;
  }

  /**
   * Get the SSL mode
   * @return The value
   */
  public String getSslMode() {
    return sslMode;
  }

  /**
   * Set the SSL mode
   * @param v The value
   */
  public void setSslMode(String v) {
    sslMode = v;
  }

  /**
   * Get the SSL password
   * @return The value
   */
  public String getSslPassword() {
    return sslPassword;
  }

  /**
   * Set the SSL password
   * @param v The value
   */
  public void setSslPassword(String v) {
    sslPassword = v;
  }

  /**
   * Get the SSL certificate file
   * @return The value
   */
  public String getSslCertificateFile() {
    return sslCertificateFile;
  }

  /**
   * Set the SSL certificate file
   * @param v The value
   */
  public void setSslCertificateFile(String v) {
    sslCertificateFile = v;
  }

  /**
   * Get the SSL key file
   * @return The value
   */
  public String getSslKeyFile() {
    return sslKeyFile;
  }

  /**
   * Set the SSL key file
   * @param v The value
   */
  public void setSslKeyFile(String v) {
    sslKeyFile = v;
  }

  /**
   * Get the SSL root certificate file
   * @return The value
   */
  public String getSslRootCertificateFile() {
    return sslRootCertificateFile;
  }

  /**
   * Set the SSL root certificate file
   * @param v The value
   */
  public void setSslRootCertificateFile(String v) {
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
  protected PGDirectConnection createConnection(String u, String p) throws SQLException {
    String url = buildUrl();
    Properties props = new Properties();

    if (u != null) {
      props.setProperty("user", u);
    }
    else if (user != null) {
      props.setProperty("user", user);
    }
    else {
      props.setProperty("user", "");
    }

    if (p != null) {
      props.setProperty("password", p);
    }
    else if (password != null) {
      props.setProperty("password", password);
    }
    else {
      props.setProperty("password", "");
    }

    if (parsedSqlCacheSize != null)
      props.setProperty(PARSED_SQL_CACHE_SIZE.getName(), Integer.toString(parsedSqlCacheSize));
    if (preparedStatementCacheSize != null)
      props.setProperty(PREPARED_STATEMENT_CACHE_SIZE.getName(), Integer.toString(preparedStatementCacheSize));
    if (applicationName != null)
      props.setProperty(APPLICATION_NAME.getName(), applicationName);
    if (clientEncoding != null)
      props.setProperty(PROTOCOL_ENCODING.getName(), clientEncoding);
    if (networkTimeout != null)
      props.setProperty(DEFAULT_NETWORK_TIMEOUT.getName(), Integer.toString(networkTimeout));
    props.setProperty(STRICT_MODE.getName(), Boolean.toString(strictMode));
    if (defaultFetchSize != null)
      props.setProperty(DEFAULT_FETCH_SIZE.getName(), Integer.toString(defaultFetchSize));

    if (receiveBufferSize != null)
      props.setProperty(PROTOCOL_SOCKET_RECV_BUFFER_SIZE.getName(), Integer.toString(receiveBufferSize));

    if (sendBufferSize != null)
      props.setProperty(PROTOCOL_SOCKET_SEND_BUFFER_SIZE.getName(), Integer.toString(sendBufferSize));

    if (sslMode != null)
      props.setProperty(SSL_MODE.getName(), sslMode);

    if (sslPassword != null)
      props.setProperty(SSL_KEY_FILE_PASSWORD.getName(), sslPassword);

    if (sslCertificateFile != null)
      props.setProperty(SSL_CRT_FILE.getName(), sslCertificateFile);

    if (sslKeyFile != null)
      props.setProperty(SSL_KEY_FILE.getName(), sslKeyFile);

    if (sslRootCertificateFile != null)
      props.setProperty(SSL_ROOT_CRT_FILE.getName(), sslRootCertificateFile);

    props.setProperty(HOUSEKEEPER.getName(), Boolean.toString(housekeeper));

    SharedRegistry.Factory sharedRegistryFactory;
    if (!REGISTRY_SHARING.get(props)) {

      sharedRegistryFactory =
          connInfo -> new SharedRegistry(connInfo.getServerInfo(), PGDataSource.class.getClassLoader());
    }
    else {

      sharedRegistryFactory =
          connInfo -> sharedRegistries.computeIfAbsent(connInfo, key -> new SharedRegistry(key.getServerInfo(), PGDataSource.class.getClassLoader()));
    }

    return ConnectionUtil.createConnection(url, props, sharedRegistryFactory);
  }

  private String buildUrl() throws SQLException {
    StringBuilder sb = new StringBuilder();

    if (getDatabaseName() == null)
       throw new SQLException("'databaseName' parameter mandatory for " + getServerName() + ":" + getPortNumber());

    sb.append("jdbc:pgsql://")
        .append(getServerName())
        .append(":")
        .append(getPortNumber())
        .append("/")
        .append(getDatabaseName());

    return sb.toString();
  }

}
