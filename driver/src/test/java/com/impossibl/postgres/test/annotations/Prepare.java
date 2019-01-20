package com.impossibl.postgres.test.annotations;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.sql.ResultSet;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Provides SQL query text for a {@link ResultSet} or {@link java.sql.PreparedStatement}
 * test parameter
 */
@Repeatable(Prepare.List.class)
@Target({TYPE, METHOD})
@Retention(RUNTIME)
public @interface Prepare {

  String name();
  String sql();
  String[] returning() default {};

  @Target({TYPE, METHOD})
  @Retention(RUNTIME)
  @interface List {

    Prepare[] value();

  }

}
