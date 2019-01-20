package com.impossibl.postgres.test.annotations;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Repeatable(Type.List.class)
@Target({TYPE, METHOD})
@Retention(RUNTIME)
public @interface Type {

  String name();
  String[] attributes();

  @Target({TYPE, METHOD})
  @Retention(RUNTIME)
  @interface List {
    Type[] value();
  }

}
