package com.impossibl.postgres.test.matchers;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RowSnapshot<T> implements Snapshot<T[]> {

  private Connection connection;
  private String table;
  private String[] columnNames;
  private Class<T> elementType;

  public RowSnapshot(Connection connection, String table, String[] columnNames, Class<T> elementType) {
    this.connection = connection;
    this.table = table;
    this.columnNames = columnNames;
    this.elementType = elementType;
  }

  @SuppressWarnings("unchecked")
  public List<T[]> take() {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT " + String.join(", ", columnNames) + " FROM " + table)) {
        List<T[]> rows = new ArrayList<>();
        while (resultSet.next()) {
          T[] columns = (T[]) Array.newInstance(elementType, columnNames.length);
          for (int c = 0; c < columns.length; ++c) {
            columns[c] = elementType.cast(resultSet.getObject(c + 1));
          }
          rows.add(columns);
        }
        return rows;
      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String[]> takeText() {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT " + String.join(", ", columnNames) + " FROM " + table)) {
        List<String[]> rows = new ArrayList<>();
        while (resultSet.next()) {
          String[] columns = new String[columnNames.length];
          for (int c = 0; c < columns.length; ++c) {
            columns[c] = resultSet.getString(c + 1);
          }
          rows.add(columns);
        }
        return rows;
      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
