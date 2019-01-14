package com.impossibl.jdbc.spy;

import java.sql.SQLException;
import java.util.Properties;

public interface ListenerFactory  {

  ConnectionListener newConnectionListener(Properties properties) throws SQLException;

}
