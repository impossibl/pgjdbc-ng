package com.impossibl.jdbc.spy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class SpyTest {

  @Test
  @Disabled
  public void testStdTracing() throws SQLException {

    try (Connection connection = DriverManager.getConnection("jdbc:spy:pgsql://localhost/test")) {

      try (Statement statement = connection.createStatement()) {
        statement.execute("SELECT 'Hello World!'");
      }
    }

  }

}
