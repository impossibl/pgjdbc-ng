package com.impossibl.postgres.test.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.sql.ResultSet;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Provides SQL query text for a {@link ResultSet} or {@link java.sql.PreparedStatement}
 * test parameter.
 */
@Target({TYPE, METHOD, PARAMETER})
@Retention(RUNTIME)
public @interface Query {
  String value() default "";
}
