package com.impossibl.postgres.test.extensions;

import com.impossibl.postgres.test.annotations.ConnectionProperty;
import com.impossibl.postgres.test.annotations.ConnectionSetting;
import com.impossibl.postgres.test.annotations.DBTest;
import com.impossibl.postgres.test.annotations.Prepare;
import com.impossibl.postgres.test.annotations.Query;

import java.lang.reflect.AnnotatedElement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

public class DBProvider implements ParameterResolver, BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

  private static final Namespace NS = Namespace.create("db");
  private static final String PREPARED_STATEMENT_KEY_PREFIX = "prepared-statement@";

  @Override
  public void beforeAll(ExtensionContext context) {
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    prepareAll(context);
  }

  @Override
  public void afterEach(ExtensionContext context) {
  }

  @Override
  public void afterAll(ExtensionContext context) {
  }

  @Override
  public boolean supportsParameter(ParameterContext paramCtx, ExtensionContext extCtx) throws ParameterResolutionException {

    Class<?> paramType = paramCtx.getParameter().getType();

    return (paramType == Connection.class) ||
        (paramType == Statement.class) ||
        (paramType == ResultSet.class && paramCtx.isAnnotated(Query.class)) ||
        (paramType == PreparedStatement.class && paramCtx.isAnnotated(Query.class)) ||
        (paramType == PreparedStatement.class && isPrepared(extCtx, paramCtx.getParameter().getName()));
  }

  @Override
  public Object resolveParameter(ParameterContext paramCtx, ExtensionContext extCtx) throws ParameterResolutionException {

    Class<?> paramType = paramCtx.getParameter().getType();

    if (paramType == Connection.class) {
      try {
        return open(extCtx);
      }
      catch (Exception e) {
        throw new ParameterResolutionException("Error resolving parameter", e);
      }
    }

    if (paramType == Statement.class) {
      return statement(extCtx);
    }

    Optional<Query> query = paramCtx.findAnnotation(Query.class);

    if (paramType == PreparedStatement.class) {
      if (query.isPresent()) {
        // Try lookup via name (alternative to parameter names)
        PreparedStatement ps = getPrepared(extCtx, query.get().value());
        if (ps == null) {
          // Prepare text provided in @Query
          ps = prepare(extCtx, query.get().value(), new String[0]);
        }
        return ps;
      }
      else {
        // Lookup via parameter name
        PreparedStatement ps = getPrepared(extCtx, paramCtx.getParameter().getName());
        if (ps == null) {
          throw new ParameterResolutionException("No @Query annotation or @Prepare annotation matching parameter: " + paramCtx.getParameter().getName());
        }
        return ps;
      }
    }

    if (paramType == ResultSet.class) {
      if (query.isPresent()) {
        // Try lookup via name (alternative to parameter names)
        PreparedStatement ps = getPrepared(extCtx, query.get().value());
        if (ps != null) {
          return query(extCtx, ps);
        }
        // Execute text provided in @Query
        return query(extCtx, query.get().value());
      }
      else {
        // Lookup vis parameter name
        PreparedStatement ps = getPrepared(extCtx, paramCtx.getParameter().getName());
        if (ps == null) {
          throw new ParameterResolutionException("No @Query annotation or @Prepare annotation matching parameter: " + paramCtx.getParameter().getName());
        }
        return query(extCtx, ps);
      }
    }

    return null;
  }

  private static boolean isPrepared(ExtensionContext ctx, String name) {
    return getPrepared(ctx, name) != null;
  }

  private static PreparedStatement getPrepared(ExtensionContext ctx, String name) {
    StatementResource resource = ctx.getStore(NS).get(PREPARED_STATEMENT_KEY_PREFIX + name, StatementResource.class);
    if (resource == null) return null;
    return (PreparedStatement) resource.statement;
  }

  private static void prepareAll(ExtensionContext ctx) {

    enumerateContexts(ctx, (curCtx, element) -> {

      for (Prepare prepare : findRepeatableAnnotations(element, Prepare.class)) {
        StatementResource statementResource = new StatementResource(prepare(ctx, prepare.sql(), prepare.returning()));
        ctx.getStore(NS).put(PREPARED_STATEMENT_KEY_PREFIX + prepare.name(), statementResource);
      }

    });

  }

  private static Statement statement(ExtensionContext ctx) {
    try {
      Connection connection = open(ctx);
      Statement statement = connection.createStatement();
      statement.closeOnCompletion();
      return statement;
    }
    catch (Exception e) {
      throw new ParameterResolutionException("Error resolving parameter", e);
    }
  }

  private static PreparedStatement prepare(ExtensionContext ctx, String sql, String[] returning) {
    try {
      Connection connection = open(ctx);
      if (returning != null && returning.length != 0) {
        return connection.prepareStatement(sql, returning);
      }
      else {
        return connection.prepareStatement(sql);
      }
    }
    catch (Exception e) {
      throw new ParameterResolutionException("Error resolving parameter", e);
    }
  }

  private static ResultSet query(ExtensionContext ctx, String sql) {
    try {
      Connection connection = open(ctx);
      Statement statement = connection.createStatement();
      statement.closeOnCompletion();
      return track(ctx, statement.executeQuery(sql));
    }
    catch (Exception e) {
      throw new ParameterResolutionException("Error resolving parameter", e);
    }
  }

  private static ResultSet query(ExtensionContext ctx, PreparedStatement ps) {
    try {
      return track(ctx, ps.executeQuery());
    }
    catch (Exception e) {
      throw new ParameterResolutionException("Error resolving parameter", e);
    }
  }

  private static ResultSet track(ExtensionContext ctx, ResultSet resultSet) {
    ctx.getStore(NS).put(resultSet.toString(), new ResultSetResource(resultSet));
    return resultSet;
  }

  private static Optional<ExtensionContext> findContext(ExtensionContext ctx, BiFunction<ExtensionContext, AnnotatedElement, Boolean> predicate) {
    if (ctx.getElement().isPresent() && predicate.apply(ctx, ctx.getElement().get())) return Optional.of(ctx);
    if (ctx.getParent().isPresent()) {
      return findContext(ctx.getParent().get(), predicate);
    }
    return Optional.empty();
  }

  private static void enumerateContexts(ExtensionContext ctx, BiConsumer<ExtensionContext, AnnotatedElement> each) {
    if (ctx.getElement().isPresent())
      each.accept(ctx, ctx.getElement().get());
    if (ctx.getParent().isPresent()) {
      enumerateContexts(ctx.getParent().get(), each);
    }
  }

  private static Properties collectProperties(ExtensionContext ctx) {
    Properties properties = new Properties();
    enumerateContexts(ctx, (c, e) -> {
      for (ConnectionProperty connectionProperty : e.getDeclaredAnnotationsByType(ConnectionProperty.class)) {
        properties.setProperty(connectionProperty.name(), connectionProperty.value());
      }
    });
    return properties;
  }

  private static List<ConnectionSetting> collectSettings(ExtensionContext ctx) {
    List<ConnectionSetting> settings = new ArrayList<>();
    enumerateContexts(ctx, (c, e) -> settings.addAll(Arrays.asList(e.getDeclaredAnnotationsByType(ConnectionSetting.class))));
    return settings;
  }

  static Connection open(ExtensionContext ctx) {

    Optional<ExtensionContext> dbTestCtxOpt =
        findContext(ctx, (c, e) -> e.isAnnotationPresent(DBTest.class));
    if (!dbTestCtxOpt.isPresent()) {
      throw new ExtensionConfigurationException("Unable to fund DBTest annotation");
    }
    ExtensionContext dbTestCtx = dbTestCtxOpt.get();

    DBTest dbTest = dbTestCtx.getElement()
        .orElseThrow(IllegalStateException::new)
        .getAnnotation(DBTest.class);

    ExtensionContext connectionCtx =
        findContext(ctx, (c, e) -> e.isAnnotationPresent(ConnectionSetting.class) || e.isAnnotationPresent(ConnectionProperty.class))
            .orElse(dbTestCtx);

    String url = "jdbc:pgsql://" + dbTest.host() + "/" + dbTest.db();

    Properties properties = collectProperties(connectionCtx);
    if (properties.getProperty("user") == null) {
      properties.setProperty("user", dbTest.user());
    }
    if (properties.getProperty("password") == null) {
      properties.setProperty("password", dbTest.password());
    }

    List<ConnectionSetting> settings = collectSettings(connectionCtx);

    return open(connectionCtx, url, properties, settings);
  }

  static Connection open(ExtensionContext ctx, String url, Properties properties, List<ConnectionSetting> setttings) {

    String key = "connection@" + ctx.getUniqueId();

    return ctx.getStore(NS).getOrComputeIfAbsent(key, key1 -> {

      try {
        Connection connection = DriverManager.getConnection(url, properties);

        // Configure any connection settings
        if (!setttings.isEmpty()) {
          try(Statement statement = connection.createStatement()) {
            for (ConnectionSetting setting : setttings) {
              statement.executeUpdate("SET " + setting.name() + " " + setting.assignment()  + " " + setting.value());
            }
          }
        }

        return new ConnectionResource(connection);
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }, ConnectionResource.class).connection;
  }

  private static void close(ExtensionContext ctx) {

    String key = "connection@" + ctx.getUniqueId();

    ConnectionResource connectionResource = ctx.getStore(NS).remove(key, ConnectionResource.class);
    if (connectionResource == null) return;

    try {
      connectionResource.connection.close();
    }
    catch (SQLException ignored) {
    }
  }

  static class ConnectionResource implements Store.CloseableResource {

    Connection connection;

    ConnectionResource(Connection connection) {
      this.connection = connection;
    }

    @Override
    public void close() throws Throwable {
      connection.close();
    }

  }

  static class StatementResource implements Store.CloseableResource {

    Statement statement;

    StatementResource(Statement statement) {
      this.statement = statement;
    }

    @Override
    public void close() throws Throwable {
      statement.close();
    }

  }

  static class ResultSetResource implements Store.CloseableResource {

    ResultSet resultSet;

    ResultSetResource(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    public void close() throws Throwable {
      resultSet.close();
    }

  }

}
