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

import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.SharedRegistry;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.JDBCSettings.HOUSEKEEPER;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_NAME;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_URL;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.channel.unix.DomainSocketAddress;

/**
 * Utility class for connection
 * @author <a href="mailto:kdubb@me.com">Kevin Wooten</a>
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
class ConnectionUtil {
  private static final String POSTGRES_UNIX_SOCKET_BASE_NAME = ".s.PGSQL";
  private static final String POSTGRES_UNIX_SOCKET_INVALID_EXT = ".lock";

  private static Logger logger = Logger.getLogger(ConnectionUtil.class.getName());

  static class ConnectionSpecifier {

    private List<SocketAddress> addresses;
    private String database;
    private Properties parameters;

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

    List<SocketAddress> getAddresses() {
      return addresses;
    }

    void prependAddress(SocketAddress v) {
      addresses.add(0, v);
    }

    void appendAddress(SocketAddress v) {
      addresses.add(v);
    }

    Properties getParameters() {
      return parameters;
    }

    void addParameter(String key, String value) {
      parameters.setProperty(key, value);
    }

    String getHosts() {

      StringBuilder hosts = new StringBuilder();

      Iterator<SocketAddress> addrIter = addresses.iterator();
      while (addrIter.hasNext()) {

        SocketAddress addr = addrIter.next();

        if (addr instanceof InetSocketAddress) {
          InetSocketAddress inetAddr = (InetSocketAddress) addr;

          hosts.append(inetAddr.getHostString());

          if (inetAddr.getPort() != 5432) {
            hosts.append(':');
            hosts.append(inetAddr.getPort());
          }
        }
        else if (addr instanceof DomainSocketAddress) {
          DomainSocketAddress domainAddr = (DomainSocketAddress) addr;
          hosts.append("unix:").append(domainAddr.path());
        }
        else {
          throw new IllegalStateException("Unsupported socket address");
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

  static PGDirectConnection createConnection(String url, Properties info, SharedRegistry.Factory sharedRegistryFactory) throws SQLException {
    ConnectionSpecifier connSpec = parseURL(url);
    if (connSpec == null) {
      return null;
    }

    Settings settings = buildSettings(connSpec, info);

    return createConnection(connSpec.addresses, settings, sharedRegistryFactory);
  }

  static PGDirectConnection createConnection(List<SocketAddress> addresses, Settings settings, SharedRegistry.Factory sharedRegistryFactory) throws SQLException {

    SQLException lastException = null;

    // Select housekeeper for connection
    Housekeeper.Ref housekeeper = null;
    if (settings.enabled(HOUSEKEEPER)) {
      housekeeper = ThreadedHousekeeper.acquire();
    }

    // Try to connect to each provided address in turn returning the first
    // successful connection
    for (SocketAddress address : addresses) {

      if (address instanceof InetSocketAddress) {
        InetSocketAddress inetAddress = (InetSocketAddress) address;

        if (inetAddress.isUnresolved()) {
          lastException = new SQLException("Connection Error: address '" + inetAddress.getHostString() + "' is unresolved", "8001");
          continue;
        }

      }
      else if (address instanceof DomainSocketAddress) {
        DomainSocketAddress domainAddress = (DomainSocketAddress) address;

        File socketFile = new File(domainAddress.path());
        if (!socketFile.exists()) {
          lastException = new SQLException("Connection Error: unix socket '" + socketFile + "' does not exist", "8001");
          continue;
        }
      }

      try {

        PGDirectConnection conn = new PGDirectConnection(address, settings, housekeeper);

        conn.init(sharedRegistryFactory);

        return conn;

      }
      catch (IOException e) {

        lastException = makeSQLException("Connection Error: ", "8001", e);
      }

    }

    //Couldn't connect so report that last exception we saw
    throw lastException;
  }

  /**
   * Combines multiple sources of properties into one group. Connection info
   * parameters take precedence over URL query parameters. Also, ensure
   * that all required parameters has some default value.
   *
   * @param connSpec Connection specification as parsed
   * @param connectInfo Connection info properties passed to connect
   * @return Single group of settings
   */
  private static Settings buildSettings(ConnectionSpecifier connSpec, Properties connectInfo) {
    Settings settings = new Settings();

    //Start by adding all parameters from the URL query string
    settings.setAll(connSpec.getParameters());

    //Add (or overwrite) parameters from the connection info
    settings.setAll(connectInfo);

    //Set PostgreSQL's database parameter from connSpec
    settings.set(DATABASE_NAME, connSpec.getDatabase());

    //Create & store URL
    settings.set(DATABASE_URL, "jdbc:pgsql://" + connSpec.getHosts() + "/" + connSpec.getDatabase());

    return settings;
  }

  /*
   * URL Pattern jdbc:pgsql:(?://((?:[a-zA-Z0-9\-.]+|\[[0-9a-f:]+])(?::(?:\d+))?(?:,(?:[a-zA-Z0-9\-.]+|\[[0-9a-f:]+])(?::(?:\d+))?)*)/)?([^?&]+)(?:[?&](.*))?
   *  Capturing Groups:
   *    1 = (host name, IPv4, IPv6 : port) pairs  (optional)
   *    2 = database name         (required)
   *    3 = parameters            (optional)
   */
  private static final Pattern URL_PATTERN =
      Pattern.compile("jdbc:pgsql:(?://((?:[a-zA-Z0-9\\-.]+|\\[[0-9a-f:]+])(?::(?:\\d+))?(?:,(?:[a-zA-Z0-9\\-.]+|\\[[0-9a-f:]+])(?::(?:\\d+))?)*)/)?([^?&/]+)(?:[?&](.*))?");

  private static final Pattern ADDRESS_PATTERN = Pattern.compile("(?:([a-zA-Z0-9\\-.]+|\\[[0-9a-f:]+])(?::(\\d+))?)");

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

      //Parse hosts into list of addresses
      String hosts = nullToEmpty(urlMatcher.group(1));
      Matcher hostsMatcher = ADDRESS_PATTERN.matcher(hosts);
      while (hostsMatcher.find()) {

        String name = hostsMatcher.group(1);

        String port = hostsMatcher.group(2);
        if (port == null || port.isEmpty()) {
          port = "5432";
        }

        InetSocketAddress address = new InetSocketAddress(name, Integer.parseInt(port));
        spec.appendAddress(address);
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

      // Add unix socket address (if specified)
      String unixSocketPath = spec.parameters.getProperty("unixsocket");
      if (unixSocketPath != null) {
        spec.parameters.remove("unixsocket");

        File unixSocketFile = new File(unixSocketPath);

        // If path is to a directory, try to find and append the PG socket
        String[] files = unixSocketFile.list((dir, name) -> name.startsWith(POSTGRES_UNIX_SOCKET_BASE_NAME) && !name.endsWith(POSTGRES_UNIX_SOCKET_INVALID_EXT));
        if (files != null && files.length != 0) {
          if (files.length != 1) {
            logger.warning("Multiple PostgreSQL unix sockets found in " + unixSocketPath + ", chose " + files[0] + " at random. Specify socket file to remove this warning.");
          }
          unixSocketFile = new File(unixSocketFile, files[0]);
        }

        // Prepend the address with the thought that if you're attempting to
        // search multiple addresses, you're preferring the unix socket.
        //
        // This can be changed to allow the address in the regular hosts list
        // if needed; an appropriate syntax will need to be determined.
        spec.prependAddress(new DomainSocketAddress(unixSocketFile));
      }

      // If no addresses specified, add localhost
      if (spec.addresses.isEmpty()) {
        spec.appendAddress(new InetSocketAddress("localhost", 5432));
      }

      logger.fine("parseURL: " + url + " => " + spec);

      return spec;

    }
    catch (Throwable e) {
      return null;
    }
  }
}
