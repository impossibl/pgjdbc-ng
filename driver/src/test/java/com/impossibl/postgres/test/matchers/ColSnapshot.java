package com.impossibl.postgres.test.matchers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ColSnapshot<T> implements Snapshot<T> {

  private Connection connection;
  private String table;
  private String column;
  private Class<T> elementType;

  public ColSnapshot(Connection connection, String table, String column, Class<T> elementType) {
    this.connection = connection;
    this.table = table;
    this.column = column;
    this.elementType = elementType;
  }

  public List<T> take() {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT " + column + " FROM " + table)) {
        List<T> values = new ArrayList<>();
        while (resultSet.next()) {
          values.add(elementType.cast(resultSet.getObject(1)));
        }
        return values;
      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> takeText() {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT " + column + "::text FROM " + table)) {
        List<String> values = new ArrayList<>();
        while (resultSet.next()) {
          values.add(resultSet.getString(1));
        }
        return values;
      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
