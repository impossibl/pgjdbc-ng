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

import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.SharedRegistry;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.PGSettings.HOUSEKEEPER;
import static com.impossibl.postgres.jdbc.PGSettings.HOUSEKEEPER_DEFAULT_DRIVER;
import static com.impossibl.postgres.jdbc.PGSettings.HOUSEKEEPER_ENABLED_LEGACY;
import static com.impossibl.postgres.system.Settings.APPLICATION_NAME;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE_URL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Boolean.parseBoolean;

/**
 * Utility class for connection
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
class ConnectionUtil {
  private static final String JDBC_APPLICATION_NAME_PARAM = "applicationName";
  private static final String JDBC_CLIENT_ENCODING_PARAM = "clientEncoding";
  private static Logger log = Logger.getLogger(ConnectionUtil.class.getName());

  static class ConnectionSpecifier {

    private List<InetSocketAddress> addresses = new ArrayList<>();
    private String database;
    private Properties parameters = new Properties();

    ConnectionSpecifier() {
      addresses = new ArrayList<>();
      database = null;
      parameters = new Properties();
    }

    String getDatabase() {
      return database;
    }

    void setDatabase(String v) {
      database = v;
    }

    List<InetSocketAddress> getAddresses() {
      return addresses;
    }

    void addAddress(InetSocketAddress v) {
      addresses.add(v);
    }

    Properties getParameters() {
      return parameters;
    }

    void addParameter(String key, String value) {
      parameters.put(key, value);
    }

    String getHosts() {

      StringBuilder hosts = new StringBuilder();

      Iterator<InetSocketAddress> addrIter = addresses.iterator();
      while (addrIter.hasNext()) {

        InetSocketAddress addr = addrIter.next();

        hosts.append(addr.getHostString());

        if (addr.getPort() != 5432) {
          hosts.append(':');
          hosts.append(addr.getPort());
        }

        if (addrIter.hasNext()) {
          hosts.append(",");
        }
      }

      return hosts.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return new StringBuilder("ConnectionSpecifier[")
          .append("hosts=").append(getHosts())
          .append(",")
          .append("database=").append(getDatabase())
          .append(",")
          .append("parameters=").append(getParameters())
          .append("]")
          .toString();
    }
  }

  static PGDirectConnection createConnection(String url, Properties info, SharedRegistry.Factory sharedRegistryFactory, boolean allowHousekeeper) throws SQLException {
    ConnectionSpecifier connSpec = parseURL(url);
    if (connSpec == null) {
      return null;
    }

    SQLException lastException = null;

    Properties settings = buildSettings(connSpec, info);

    // Select housekeeper for connection
    Housekeeper.Ref housekeeper = null;
    if (allowHousekeeper) {
      boolean enableHousekeeper = true;
      if (settings.getProperty(HOUSEKEEPER_ENABLED_LEGACY) != null &&
          !parseBoolean(settings.getProperty(HOUSEKEEPER_ENABLED_LEGACY))) {
        enableHousekeeper = false;
      }
      else if (!parseBoolean(settings.getProperty(HOUSEKEEPER, HOUSEKEEPER_DEFAULT_DRIVER))) {
        enableHousekeeper = false;
      }
      if (enableHousekeeper)
        housekeeper = ThreadedHousekeeper.acquire();
    }

    // Try to connect to each provided address in turn returning the first
    // successful connection
    for (InetSocketAddress address : connSpec.getAddresses()) {

      if (address.isUnresolved()) {
        lastException = new SQLException("Connection Error: address '" + address.getHostString() + "' is unresolved", "8001");
        continue;
      }

      try {

        PGDirectConnection conn = new PGDirectConnection(address, settings, housekeeper);

        conn.init(sharedRegistryFactory);

        return conn;

      }
      catch (IOException e) {

        lastException = new SQLException("Connection Error: " + e.getMessage(), "8001", e);
      }
      catch (NoticeException e) {

        lastException = makeSQLException("Connection Error: ", e.getNotice());
      }
    }

    //Couldn't connect so report that last exception we saw
    throw lastException;
  }

  /**
   * Combines multiple sources of properties into one group. Connection info
   * parameters take precedence over URL query parameters. Also, it ensure
   * that all required parameters has some default value.
   *
   * @param connSpec Connection specification as parsed
   * @param connectInfo Connection info properties passed to connect
   * @return Single group of settings
   */
  private static Properties buildSettings(ConnectionSpecifier connSpec, Properties connectInfo) {
    Properties settings = new Properties();

    //Start by adding all parameters from the URL query string
    settings.putAll(connSpec.getParameters());

    //Add (or overwrite) parameters from the connection info
    settings.putAll(connectInfo);

    //Set PostgreSQL's database parameter from connSpec
    settings.put("database", connSpec.getDatabase());

    //Translate JDBC parameters to PostgreSQL parameters
    if (settings.getProperty(CREDENTIALS_USERNAME) == null)
      settings.put(CREDENTIALS_USERNAME, "");
    if (settings.getProperty(CREDENTIALS_PASSWORD) == null)
      settings.put(CREDENTIALS_PASSWORD, "");
    if (settings.getProperty(APPLICATION_NAME) == null && settings.getProperty(JDBC_APPLICATION_NAME_PARAM) != null)
      settings.put(APPLICATION_NAME, settings.getProperty(JDBC_APPLICATION_NAME_PARAM));
    if (settings.getProperty(CLIENT_ENCODING) == null && settings.getProperty(JDBC_CLIENT_ENCODING_PARAM) != null)
      settings.put(CLIENT_ENCODING, settings.getProperty(JDBC_CLIENT_ENCODING_PARAM));

    //Create & store URL
    settings.put(DATABASE_URL, "jdbc:pgsql://" + connSpec.getHosts() + "/" + connSpec.getDatabase());

    return settings;
  }

  /*
   * URL Pattern jdbc:pgsql:(?://((?:[a-zA-Z0-9\-\.]+|\[[0-9a-f\:]+\])(?:\:(?:\d+))?(?:,(?:[a-zA-Z0-9\-\.]+|\[[0-9a-f\:]+\])(?:\:(?:\d+))?)*)/)?(\w+)(?:\?(.*))?
   *  Capturing Groups:
   *    1 = (host name, IPv4, IPv6 : port) pairs  (optional)
   *    2 = database name         (required)
   *    3 = parameters            (optional)
   */
  private static final Pattern URL_PATTERN =
      Pattern.compile("jdbc:pgsql:(?://((?:[a-zA-Z0-9\\-\\.]+|\\[[0-9a-f\\:]+\\])(?:\\:(?:\\d+))?(?:,(?:[a-zA-Z0-9\\-\\.]+|\\[[0-9a-f\\:]+\\])(?:\\:(?:\\d+))?)*)/)?((?:\\w|-|_)+)(?:[\\?\\&](.*))?");

  private static final Pattern ADDRESS_PATTERN = Pattern.compile("(?:([a-zA-Z0-9\\-\\.]+|\\[[0-9a-f\\:]+\\])(?:\\:(\\d+))?)");

  /**
   * Parses a URL connection string.
   * 
   * Uses the URL_PATTERN to capture a hostname or ip address, port, database
   * name and a list of parameters specified as query name=value pairs. All
   * parts but the database name are optional.
   * 
   * @param url
   *          Connection URL to parse
   * @return Connection specifier of parsed URL
   */
  static ConnectionSpecifier parseURL(String url) {

    try {

      //First match aginst the entire URL pattern.  If that doesn't work
      //then the url is invalid

      Matcher urlMatcher = URL_PATTERN.matcher(url);
      if (!urlMatcher.matches()) {
        return null;
      }

      //Now build a conn-spec from the optional pieces of the URL
      //
      ConnectionSpecifier spec = new ConnectionSpecifier();

      //Get hosts, if provided, or use the default "localhost:5432"
      String hosts = urlMatcher.group(1);
      if (hosts == null || hosts.isEmpty()) {
        hosts = "localhost";
      }

      //Parse hosts into list of addresses
      Matcher hostsMatcher = ADDRESS_PATTERN.matcher(hosts);
      while (hostsMatcher.find()) {

        String name = hostsMatcher.group(1);

        String port = hostsMatcher.group(2);
        if (port == null || port.isEmpty()) {
          port = "5432";
        }

        InetSocketAddress address = new InetSocketAddress(name, Integer.parseInt(port));
        spec.addAddress(address);
      }


      //Assign the database

      spec.setDatabase(urlMatcher.group(2));

      //Parse the query string as a list of name=value pairs separated by '&'
      //then assign them as extra parameters

      String params = urlMatcher.group(3);
      if (params != null && !params.isEmpty()) {

        for (String nameValue : params.split("&")) {

          String[] items = nameValue.split("=");

          if (items.length == 1) {
            spec.addParameter(items[0], "");
          }
          else if (items.length == 2) {
            spec.addParameter(items[0], items[1]);
          }
        }
      }

      log.fine("parseURL: " + url + " => " + spec);

      return spec;

    }
    catch (Throwable e) {
      return null;
    }
  }
}
