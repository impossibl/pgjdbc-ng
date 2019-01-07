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
import com.impossibl.postgres.system.Setting;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.SharedRegistry;

import static com.impossibl.postgres.jdbc.DataSourceSettings.DATABASE_NAME;
import static com.impossibl.postgres.jdbc.DataSourceSettings.DATASOURCE_NAME;
import static com.impossibl.postgres.jdbc.DataSourceSettings.DS;
import static com.impossibl.postgres.jdbc.DataSourceSettings.LOGIN_TIMEOUT;
import static com.impossibl.postgres.jdbc.DataSourceSettings.PORT_NUMBER;
import static com.impossibl.postgres.jdbc.DataSourceSettings.SERVER_NAME;
import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_FETCH_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.DEFAULT_NETWORK_TIMEOUT;
import static com.impossibl.postgres.jdbc.JDBCSettings.DESCRIPTION_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.HOUSEKEEPER;
import static com.impossibl.postgres.jdbc.JDBCSettings.JDBC;
import static com.impossibl.postgres.jdbc.JDBCSettings.PARSED_SQL_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.PREPARED_STATEMENT_CACHE_SIZE;
import static com.impossibl.postgres.jdbc.JDBCSettings.PREPARED_STATEMENT_CACHE_THRESHOLD;
import static com.impossibl.postgres.jdbc.JDBCSettings.READ_ONLY;
import static com.impossibl.postgres.jdbc.JDBCSettings.REGISTRY_SHARING;
import static com.impossibl.postgres.jdbc.JDBCSettings.STRICT_MODE;
import static com.impossibl.postgres.system.SystemSettings.APPLICATION_NAME;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_URL;
import static com.impossibl.postgres.system.SystemSettings.FIELD_FORMAT_PREF;
import static com.impossibl.postgres.system.SystemSettings.FIELD_LENGTH_MAX;
import static com.impossibl.postgres.system.SystemSettings.MONEY_FRACTIONAL_DIGITS;
import static com.impossibl.postgres.system.SystemSettings.PARAM_FORMAT_PREF;
import static com.impossibl.postgres.system.SystemSettings.PROTO;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_BUFFER_POOLING;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_ENCODING;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_IO_MODE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_IO_THREADS;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_MESSAGE_SIZE_MAX;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_RECV_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_SOCKET_SEND_BUFFER_SIZE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_TRACE;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_VERSION;
import static com.impossibl.postgres.system.SystemSettings.SQL_TRACE;
import static com.impossibl.postgres.system.SystemSettings.SSL_CA_CRT_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_CRT_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_HOME_DIR;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_PASSWORD_CALLBACK;
import static com.impossibl.postgres.system.SystemSettings.SSL_MODE;
import static com.impossibl.postgres.system.SystemSettings.SYS;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.util.Collections.singletonList;

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

  private Settings settings = new Settings(DS, JDBC, SYS, PROTO);

  private Map<ServerConnectionInfo, SharedRegistry> sharedRegistries;

  /**
   * Constructor
   */
  protected AbstractDataSource() {
    this.sharedRegistries = new ConcurrentHashMap<>();
  }

  /**
   * Create a connection
   *
   * @param username
   *          The user name
   * @param password
   *          The password
   * @return The connection
   * @exception SQLException
   *              Thrown in case of an error
   */
  protected PGDirectConnection createConnection(String username, String password) throws SQLException {

    settings.set(CREDENTIALS_USERNAME, username);
    settings.set(CREDENTIALS_PASSWORD, password);

    SharedRegistry.Factory sharedRegistryFactory;
    if (!settings.enabled(REGISTRY_SHARING)) {

      sharedRegistryFactory =
          connInfo -> new SharedRegistry(connInfo.getServerInfo(), PGDataSource.class.getClassLoader());
    }
    else {

      sharedRegistryFactory =
          connInfo -> sharedRegistries.computeIfAbsent(connInfo, key -> new SharedRegistry(key.getServerInfo(), PGDataSource.class.getClassLoader()));
    }

    String url = settings.get(DATABASE_URL);
    if (url != null) {

      // Ensure no overlapping settings are stored
      settings.unset(SERVER_NAME);
      settings.unset(PORT_NUMBER);
      settings.unset(DATABASE_NAME);

      return ConnectionUtil.createConnection(url, settings.asProperties(), sharedRegistryFactory);
    }
    else {

      // Store the URL give the provided settings
      url = "jdbc:pgsql://" + settings.get(SERVER_NAME) + ":" + settings.get(PORT_NUMBER) + "/" + settings.get(DATABASE_NAME);
      settings.set(DATABASE_URL, url);

      SocketAddress address = new InetSocketAddress(settings.get(SERVER_NAME), settings.get(PORT_NUMBER));

      return ConnectionUtil.createConnection(singletonList(address), settings, sharedRegistryFactory);
    }

  }

  /**
   * Create a reference using the correct ObjectFactory instance
   * @return The reference
   */
  protected abstract Reference createReference();

  private void addRefAddrIfSet(Reference ref, Setting<?> setting) {
    if (!settings.hasStoredValue(setting)) return;
    ref.add(new StringRefAddr(setting.getBeanPropertyName(), settings.getText(setting)));
  }

  private void addRefAddrIfMissing(Reference ref, Setting<?> setting) {
    if (settings.hasStoredValue(setting)) return;
    ref.add(new StringRefAddr(setting.getBeanPropertyName(), settings.getText(setting)));
  }

  /**
   * {@inheritDoc}
   */
  public Reference getReference() throws NamingException {
    Reference ref = createReference();

    for (Setting<?> setting : settings.knownSet()) {
      addRefAddrIfSet(ref, setting);
    }

    addRefAddrIfMissing(ref, DATASOURCE_NAME);
    addRefAddrIfMissing(ref, SERVER_NAME);
    addRefAddrIfMissing(ref, PORT_NUMBER);
    addRefAddrIfMissing(ref, DATABASE_NAME);

    return ref;
  }

  /**
   * Init
   * @param reference The reference
   */
  public void init(Reference reference) {

    for (Setting<?> setting : settings.knownSet()) {
      String value = getReferenceValue(reference, setting.getBeanPropertyName());
      if (value != null) {
        settings.setText(setting, value);
      }
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

  public abstract String getDescription();

  @Override
  public int getLoginTimeout() throws SQLException {
    return settings.get(LOGIN_TIMEOUT);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    settings.set(LOGIN_TIMEOUT, seconds);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    // Not supported
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // Not supported
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(Context.class.getPackage().getName());
  }

  /******
   * Property to setting mapping methods.
   *
   * These are purposefully not documented because settings are
   * themselves documented and it is a maintenance hassle to
   * duplicate the documentation.
   *
   ******/

  // TODO This section should be auto-generated

  public String getDatabaseUrl() {
    return settings.get(DATABASE_URL);
  }

  public void setDatabaseUrl(String v) {
    settings.set(DATABASE_URL, v);
  }

  public String getDataSourceName() {
    return settings.get(DATASOURCE_NAME);
  }

  public void setDataSourceName(String v) {
    settings.set(DATASOURCE_NAME, v);
  }

  public String getServerName() {
    return settings.get(SERVER_NAME);
  }

  public void setServerName(String v) {
    settings.set(SERVER_NAME, v);
  }

  public int getPortNumber() {
    return settings.get(PORT_NUMBER);
  }

  public void setPortNumber(int v) {
    settings.set(PORT_NUMBER, v);
  }

  public String getDatabaseName() {
    return settings.get(DATABASE_NAME);
  }

  public void setDatabaseName(String v) {
    settings.set(DATABASE_NAME, v);
  }

  public String getDatabase() {
    return settings.get(DATABASE_NAME);
  }

  public void setDatabase(String v) {
    settings.set(DATABASE_NAME, v);
  }

  public String getUser() {
    return settings.get(CREDENTIALS_USERNAME);
  }

  public void setUser(String v) {
    settings.set(CREDENTIALS_USERNAME, v);
  }

  public String getPassword() {
    return settings.get(CREDENTIALS_PASSWORD);
  }

  public void setPassword(String v) {
    settings.set(CREDENTIALS_PASSWORD, v);
  }

  public String getApplicationName() {
    return settings.get(APPLICATION_NAME);
  }

  public void setApplicationName(String v) {
    settings.set(APPLICATION_NAME, v);
  }

  public boolean getHousekeeper() {
    return settings.get(HOUSEKEEPER);
  }

  public void setHousekeeper(boolean v) {
    settings.set(HOUSEKEEPER, v);
  }

  public boolean getRegistrySharing() {
    return settings.get(REGISTRY_SHARING);
  }

  public void setRegistrySharing(boolean v) {
    settings.set(REGISTRY_SHARING, v);
  }

  public int getParsedSqlCacheSize() {
    return settings.get(PARSED_SQL_CACHE_SIZE);
  }

  public void setParsedSqlCacheSize(int v) {
    settings.set(PARSED_SQL_CACHE_SIZE, v);
  }

  public int getPreparedStatementCacheSize() {
    return settings.get(PREPARED_STATEMENT_CACHE_SIZE);
  }

  public void setPreparedStatementCacheSize(int v) {
    settings.set(PREPARED_STATEMENT_CACHE_SIZE, v);
  }

  public int getPreparedStatementCacheThreshold() {
    return settings.get(PREPARED_STATEMENT_CACHE_THRESHOLD);
  }

  public void setPreparedStatementCacheThreshold(int v) {
    settings.set(PREPARED_STATEMENT_CACHE_THRESHOLD, v);
  }

  public int getDescriptionCacheSize() {
    return settings.get(DESCRIPTION_CACHE_SIZE);
  }

  public void setDescriptionCacheSize(int v) {
    settings.set(DESCRIPTION_CACHE_SIZE, v);
  }

  public boolean getReadOnly() {
    return settings.get(READ_ONLY);
  }

  public void setReadOnly(boolean v) {
    settings.set(READ_ONLY, v);
  }

  public boolean getStrictMode() {
    return settings.get(STRICT_MODE);
  }

  public void setStrictMode(boolean v) {
    settings.set(STRICT_MODE, v);
  }

  public int getNetworkTimeout() {
    return settings.get(DEFAULT_NETWORK_TIMEOUT);
  }

  public void setNetworkTimeout(int v) {
    settings.set(DEFAULT_NETWORK_TIMEOUT, v);
  }

  public int getFetchSize() {
    return settings.get(DEFAULT_FETCH_SIZE);
  }

  public void setFetchSize(int v) {
    settings.set(DEFAULT_FETCH_SIZE, v);
  }

  public String getFieldFormat() {
    return settings.getText(FIELD_FORMAT_PREF);
  }

  public void setFieldFormat(String v) {
    settings.setText(FIELD_FORMAT_PREF, v);
  }

  public int getFieldLengthMax() {
    return settings.get(FIELD_LENGTH_MAX);
  }

  public void setFieldLengthMax(int v) {
    settings.set(FIELD_LENGTH_MAX, v);
  }

  public String getParamFormat() {
    return settings.getText(PARAM_FORMAT_PREF);
  }

  public void setParamFormat(String v) {
    settings.setText(PARAM_FORMAT_PREF, v);
  }

  public int getMoneyFractionalDigits() {
    return settings.get(MONEY_FRACTIONAL_DIGITS);
  }

  public void setMoneyFractionalDigits(int v) {
    settings.set(MONEY_FRACTIONAL_DIGITS, v);
  }

  public String getSslMode() {
    return settings.getText(SSL_MODE);
  }

  public void setSslMode(String v) {
    settings.setText(SSL_MODE, v);
  }

  public String getSslCertificateFile() {
    return settings.get(SSL_CRT_FILE);
  }

  public void setSslCertificateFile(String v) {
    settings.set(SSL_CRT_FILE, v);
  }

  public String getSslCaCertificateFile() {
    return settings.get(SSL_CA_CRT_FILE);
  }

  public void setSslCaCertificateFile(String v) {
    settings.set(SSL_CA_CRT_FILE, v);
  }

  public String getSslKeyFile() {
    return settings.get(SSL_KEY_FILE);
  }

  public void setSslKeyFile(String v) {
    settings.set(SSL_KEY_FILE, v);
  }

  public String getSslKeyPassword() {
    return settings.get(SSL_KEY_PASSWORD);
  }

  public void setSslKeyPassword(String v) {
    settings.setText(SSL_KEY_PASSWORD, v);
  }

  public String getSslKeyPasswordCallback() {
    return settings.getText(SSL_KEY_PASSWORD_CALLBACK);
  }

  public void setSslKeyPasswordCallback(String v) {
    settings.setText(SSL_KEY_PASSWORD_CALLBACK, v);
  }

  public String getSslHomeDir() {
    return settings.get(SSL_HOME_DIR);
  }

  public void setSslHomeDir(String v) {
    settings.set(SSL_HOME_DIR, v);
  }

  public boolean getSqlTrace() {
    return settings.get(SQL_TRACE);
  }

  public void setSqlTrace(boolean v) {
    settings.set(SQL_TRACE, v);
  }

  public String getProtocolVersion() {
    return settings.getText(PROTOCOL_VERSION);
  }

  public void setProtocolVersion(String v) {
    settings.setText(PROTOCOL_VERSION, v);
  }

  public String getProtocolIoMode() {
    return settings.getText(PROTOCOL_IO_MODE);
  }

  public void setProtocolIoMode(String v) {
    settings.setText(PROTOCOL_IO_MODE, v);
  }

  public int getProtocolIoThreads() {
    return settings.get(PROTOCOL_IO_THREADS);
  }

  public void setProtocolIoThreads(int v) {
    settings.set(PROTOCOL_IO_THREADS, v);
  }

  public String getProtocolEncoding() {
    return settings.getText(PROTOCOL_ENCODING);
  }

  public void setProtocolEncoding(String v) {
    settings.setText(PROTOCOL_ENCODING, v);
  }

  public int getProtocolSocketRecvBufferSize() {
    return settings.get(PROTOCOL_SOCKET_RECV_BUFFER_SIZE);
  }

  public void setProtocolSocketRecvBufferSize(int v) {
    settings.set(PROTOCOL_SOCKET_RECV_BUFFER_SIZE, v);
  }

  public int getProtocolSocketSendBufferSize() {
    return settings.get(PROTOCOL_SOCKET_SEND_BUFFER_SIZE);
  }

  public void setProtocolSocketSendBufferSize(int v) {
    settings.set(PROTOCOL_SOCKET_SEND_BUFFER_SIZE, v);
  }

  public boolean getProtocolBufferPooling() {
    return settings.get(PROTOCOL_BUFFER_POOLING);
  }

  public void setProtocolBufferPooling(boolean v) {
    settings.set(PROTOCOL_BUFFER_POOLING, v);
  }

  public int getProtocolMessageSizeMax() {
    return settings.get(PROTOCOL_MESSAGE_SIZE_MAX);
  }

  public void setProtocolMessageSizeMax(int v) {
    settings.set(PROTOCOL_MESSAGE_SIZE_MAX, v);
  }

  public boolean getProtocolTrace() {
    return settings.get(PROTOCOL_TRACE);
  }

  public void setProtocolTrace(boolean v) {
    settings.set(PROTOCOL_TRACE, v);
  }


  // host, alias for serverName

  public String getHost() {
    return getServerName();
  }

  public void setHost(String v) {
    setServerName(v);
  }


  // port, alias for portNumber

  public int getPort() {
    return getPortNumber();
  }

  public void setPort(int v) {
    setPortNumber(v);
  }


  // clientEncoding, alias for protocolEncoding

  public String getClientEncoding() {
    return getProtocolEncoding();
  }

  public void setClientEncoding(String v) {
    setProtocolEncoding(v);
  }

}
