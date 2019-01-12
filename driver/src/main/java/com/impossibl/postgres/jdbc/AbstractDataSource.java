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
import static com.impossibl.postgres.jdbc.JDBCSettings.JDBC;
import static com.impossibl.postgres.jdbc.JDBCSettings.REGISTRY_SHARING;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.SystemSettings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_URL;
import static com.impossibl.postgres.system.SystemSettings.PROTO;
import static com.impossibl.postgres.system.SystemSettings.PROTOCOL_ENCODING;
import static com.impossibl.postgres.system.SystemSettings.SYS;
import static com.impossibl.postgres.utils.StringTransforms.toLowerCamelCase;

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

  protected Settings settings = new Settings(DS, JDBC, SYS, PROTO);

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

      PGDirectConnection connection = ConnectionUtil.createConnection(url, settings.asProperties(), sharedRegistryFactory);
      if (connection == null) {
        throw new SQLException("Unsupported database URL");
      }

      return connection;
    }
    else {

      // Store the URL given the provided settings
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
    ref.add(new StringRefAddr(toLowerCamelCase(setting.getName()), settings.getText(setting)));
  }

  private void addRefAddrIfMissing(Reference ref, Setting<?> setting) {
    if (settings.hasStoredValue(setting)) return;
    ref.add(new StringRefAddr(toLowerCamelCase(setting.getName()), settings.getText(setting)));
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
      String value = getReferenceValue(reference, toLowerCamelCase(setting.getName()));
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


  // host, alias for serverName

  public String getHost() {
    return settings.get(SERVER_NAME);
  }

  public void setHost(String v) {
    settings.set(SERVER_NAME, v);
  }


  // port, alias for portNumber

  public int getPort() {
    return settings.get(PORT_NUMBER);
  }

  public void setPort(int v) {
    settings.set(PORT_NUMBER, v);
  }


  // clientEncoding, alias for protocolEncoding

  public String getClientEncoding() {
    return settings.getText(PROTOCOL_ENCODING);
  }

  public void setClientEncoding(String v) {
    settings.setText(PROTOCOL_ENCODING, v);
  }

}
