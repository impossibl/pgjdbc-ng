package com.impossibl.jdbc.spy;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class SpyDriver implements Driver {

  private static SpyDriver registered;

  static {

    try {
      registered = new SpyDriver();
      DriverManager.registerDriver(registered);
    }
    catch (SQLException e) {

    }

  }

  private String getTargetDriverUrl(String url) {
    return "jdbc" + url.substring(8);
  }

  private Driver getTargetDriver(String url) throws SQLException {
    return DriverManager.getDriver(getTargetDriverUrl(url));
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!url.startsWith("jdbc:spy")) return null;

    Driver driver = getTargetDriver(url);

    String spyFactoryClassName =
        info.getProperty("spy.factory", System.getProperty("spy.factory", TracerFactory.class.getName()));
    info.remove("spy.factory");

    ListenerFactory spyFactory;
    try {
      Class<?> spyFactoryClass = Class.forName(spyFactoryClassName);
      spyFactory = (ListenerFactory) spyFactoryClass.getConstructor().newInstance();
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new SQLException("SPY: Unable to instantiate Spy ListenerFactory: " + spyFactoryClassName, e);
    }

    ConnectionListener connectionListener = spyFactory.newConnectionListener(info);

    Connection connection = driver.connect(getTargetDriverUrl(url), info);

    return new ConnectionRelay(connection, connectionListener);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith("jdbc:spy") && getTargetDriver(url).acceptsURL(getTargetDriverUrl(url));
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    DriverPropertyInfo[] propertyInfos = getTargetDriver(url).getPropertyInfo(url, info);
    propertyInfos = Arrays.copyOf(propertyInfos, propertyInfos.length + 1);
    propertyInfos[propertyInfos.length - 1] = new DriverPropertyInfo("spy.trace.url", info.getProperty("spy.trace.url"));
    return propertyInfos;
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

}
