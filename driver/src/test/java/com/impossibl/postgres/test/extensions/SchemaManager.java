package com.impossibl.postgres.test.extensions;

import com.impossibl.postgres.test.annotations.Schema;
import com.impossibl.postgres.test.annotations.Table;
import com.impossibl.postgres.test.annotations.Type;
import com.impossibl.postgres.test.matchers.ColSnapshot;
import com.impossibl.postgres.test.matchers.RowSnapshot;

import static com.impossibl.postgres.test.extensions.DBProvider.open;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

public class SchemaManager implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback, ParameterResolver {

  private static final Namespace NS = Namespace.create("schema");

  private static final TypeToken<ColSnapshot<?>> COL_SNAPSHOT_TYPE = new TypeToken<ColSnapshot<?>>() {};
  private static final TypeToken<RowSnapshot<?>> ROW_SNAPSHOT_TYPE = new TypeToken<RowSnapshot<?>>() {};

  @Override
  public boolean supportsParameter(ParameterContext paramCtx, ExtensionContext extCtx) throws ParameterResolutionException {
    TypeToken<?> paramType = TypeToken.of(paramCtx.getParameter().getParameterizedType());
    String paramName = paramCtx.getParameter().getName();
    SchemaTracker tracker = getTracker(extCtx);

    return (COL_SNAPSHOT_TYPE.isSupertypeOf(paramType) || ROW_SNAPSHOT_TYPE.isSupertypeOf(paramType)) && tracker.find(paramName, Table.class) != null;
  }

  @Override
  public Object resolveParameter(ParameterContext paramCtx, ExtensionContext extCtx) throws ParameterResolutionException {

    TypeToken<?> paramType = TypeToken.of(paramCtx.getParameter().getParameterizedType());
    String paramName = paramCtx.getParameter().getName();
    SchemaTracker tracker = getTracker(extCtx);

    if (COL_SNAPSHOT_TYPE.isSupertypeOf(paramType)) {
      Class<?> elementType = paramType.resolveType(paramCtx.getParameter().getType().getTypeParameters()[0]).getRawType();
      Table table = tracker.find(paramName, Table.class);
      if (table == null) {
        throw new ParameterResolutionException("Unable to locate table definition for " + paramName);
      }
      String[] columnNames = Arrays.stream(table.columns()).map(col -> col.split(" ")[0]).toArray(String[]::new);
      return new ColSnapshot<>(DBProvider.open(extCtx), table.name(), columnNames[0], elementType);
    }

    if (ROW_SNAPSHOT_TYPE.isSupertypeOf(paramType)) {
      Class<?> elementType = paramType.resolveType(paramCtx.getParameter().getType().getTypeParameters()[0]).getRawType();
      Table table = tracker.find(paramName, Table.class);
      if (table == null) {
        throw new ParameterResolutionException("Unable to locate table definition for " + paramName);
      }
      String[] columnNames = Arrays.stream(table.columns()).map(col -> col.split(" ")[0]).toArray(String[]::new);
      return new RowSnapshot<>(DBProvider.open(extCtx), table.name(), columnNames, elementType);
    }

    return null;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    createSchemaObjects(context);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    createSchemaObjects(extensionContext);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    dropSchemaObjects(extensionContext);

    try (Statement statement = open(extensionContext).createStatement()) {
      getTracker(extensionContext).truncateAllTables(statement);
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    dropSchemaObjects(context);
  }

  private static SchemaTracker getTracker(ExtensionContext extCtx) {
    return extCtx.getRoot().getStore(NS).getOrComputeIfAbsent(SchemaTracker.class);
  }

  interface SchemaIterator {
    void apply(Schema schema) throws Exception;
    void apply(Table table) throws Exception;
    void apply(Type type) throws Exception;
  }

  private static void iterateSchemaObjects(ExtensionContext extCtx, SchemaIterator iterator) throws Exception {

    AnnotatedElement element = extCtx.getElement().orElseThrow(IllegalStateException::new);

    for (Schema schema : findRepeatableAnnotations(element, Schema.class)) {
      iterator.apply(schema);
    }

    for (Type type : findRepeatableAnnotations(element, Type.class)) {
      iterator.apply(type);
    }

    for (Table table : findRepeatableAnnotations(element, Table.class)) {
      iterator.apply(table);
    }

  }

  private static void createSchemaObjects(ExtensionContext extCtx) throws Exception {

    SchemaTracker tracker = getTracker(extCtx);

    try (Statement statement = open(extCtx).createStatement()) {
      iterateSchemaObjects(extCtx, new SchemaIterator() {
        @Override
        public void apply(Schema schema) throws Exception {
          tracker.create(statement, schema);
        }

        @Override
        public void apply(Table table) throws Exception {
          tracker.create(statement, table);
        }

        @Override
        public void apply(Type type) throws Exception {
          tracker.create(statement, type);
        }
      });
    }

  }

  private static void dropSchemaObjects(ExtensionContext extCtx) throws Exception {

    SchemaTracker tracker = getTracker(extCtx);

    try (Statement statement = open(extCtx).createStatement()) {
      iterateSchemaObjects(extCtx, new SchemaIterator() {
        @Override
        public void apply(Schema schema) throws Exception {
          tracker.drop(statement, schema);
        }

        @Override
        public void apply(Table table) throws Exception {
          tracker.drop(statement, table);
        }

        @Override
        public void apply(Type type) throws Exception {
          tracker.drop(statement, type);
        }
      });
    }

  }

  static class SchemaTracker {

    Map<String, Annotation> objects = new HashMap<>();

    <T extends Annotation> T find(String name, Class<T> annType) {
      return annType.cast(objects.get(annType.getSimpleName().toUpperCase() + "@" + name));
    }

    void create(Statement statement, Schema schema) throws SQLException {
      statement.executeUpdate("DROP SCHEMA IF EXISTS " + schema.value() + "; " +
          "CREATE SCHEMA " + schema.value() + " ");
      add(schema);
    }

    void create(Statement statement, Table table) throws SQLException {
      statement.executeUpdate("DROP TABLE IF EXISTS " + table.name() + "; " +
          "CREATE TABLE " + table.name() + " (" + String.join(", ", table.columns()) + ")");
      add(table);
    }

    void create(Statement statement, Type type) throws SQLException {
      statement.executeUpdate("DROP TYPE IF EXISTS " + type.name() + "; " +
          "CREATE TYPE " + type.name() + " (" + String.join(", ", type.attributes()) + ")");
      add(type);
    }

    void drop(Statement statement, Schema schema) throws SQLException {
      statement.executeUpdate("DROP SCHEMA " + schema.value() + " CASCADE");
      remove(schema);
    }

    void drop(Statement statement, Table table) throws SQLException {
      statement.executeUpdate("DROP TABLE " + table.name() + " CASCADE");
      remove(table);
    }

    void drop(Statement statement, Type type) throws SQLException {
      statement.executeUpdate("DROP TYPE " + type.name() + " CASCADE");
      remove(type);
    }

    private void add(Annotation object) {
      objects.put(typeOf(object) + "@" + nameOf(object), object);
    }

    private void remove(Annotation object) {
      objects.remove(typeOf(object) + "@" + nameOf(object));
    }

    static String typeOf(Annotation annotation) {
      return annotation.annotationType().getSimpleName().toUpperCase();
    }

    static String nameOf(Annotation annotation) {
      if (annotation instanceof Schema) return ((Schema) annotation).value();
      if (annotation instanceof Table) return ((Table) annotation).name();
      if (annotation instanceof Type) return ((Type) annotation).name();
      throw new IllegalStateException("Unsupported schema object type: " + annotation.annotationType().getCanonicalName());
    }

    void truncateAllTables(Statement statement) throws SQLException {
      for (Annotation object : objects.values()) {
        if (object instanceof Table) {
          statement.executeUpdate("TRUNCATE " + ((Table) object).name());
        }
      }
    }
  }

}

