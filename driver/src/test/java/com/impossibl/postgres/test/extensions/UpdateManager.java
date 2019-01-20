package com.impossibl.postgres.test.extensions;

import com.impossibl.postgres.test.annotations.Execute;
import com.impossibl.postgres.test.annotations.Insert;

import static com.impossibl.postgres.test.extensions.DBProvider.open;

import java.lang.reflect.AnnotatedElement;
import java.sql.SQLException;
import java.sql.Statement;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

public class UpdateManager implements BeforeAllCallback, BeforeEachCallback {

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    executeAll(context);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    executeAll(extensionContext);
  }

  private static void executeAll(ExtensionContext extCtx) throws Exception {

    AnnotatedElement element = extCtx.getElement().orElseThrow(IllegalStateException::new);

    try (Statement statement = open(extCtx).createStatement()) {

      for (Insert insert : findRepeatableAnnotations(element, Insert.class)) {
        execute(statement, insert);
      }

      for (Execute execute : findRepeatableAnnotations(element, Execute.class)) {
        execute(statement, execute);
      }

    }

  }

  private static void execute(Statement statement, Insert insert) throws SQLException {
    String columnList = insert.columns().length != 0 ? "(" + String.join(",", insert.columns()) + ")" : "";
    String dataCast = !insert.type().equals("") ? "::" + insert.type() : "";
    statement.executeUpdate("INSERT INTO " + insert.table() + columnList + " VALUES (" + stream(insert.values()).map(val -> "'" + val + "'" + dataCast).collect(joining(",")) + ")");
  }

  private static void execute(Statement statement, Execute execute) throws SQLException {
    statement.executeUpdate(execute.sql());
  }

}
