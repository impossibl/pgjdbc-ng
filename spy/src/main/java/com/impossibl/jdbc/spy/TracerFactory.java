package com.impossibl.jdbc.spy;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Properties;

public class TracerFactory implements ListenerFactory {

  @Override
  public ConnectionListener newConnectionListener(Properties info) throws SQLException {

    String tracerClassName =
        info.getProperty("spy.tracer", System.getProperty("spy.tracer", FileTraceOutput.class.getName()));
    info.remove("spy.tracer");


    try {
      Class<?> tracerClass = Class.forName(tracerClassName);
      TraceOutput tracer = (TraceOutput) tracerClass.getConstructor(Properties.class).newInstance(info);
      return new ConnectionTracer(tracer);
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new SQLException("SPY: Unable to instantiate Spy tracer: " + tracerClassName);
    }
  }

}
