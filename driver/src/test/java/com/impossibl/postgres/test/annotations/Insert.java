package com.impossibl.postgres.test.annotations;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Repeatable(Insert.List.class)
@Target({TYPE, METHOD})
@Retention(RUNTIME)
public @interface Insert {

  String table();
  String[] columns() default {};
  String type() default "";
  String[] values();


  @Target({TYPE, METHOD})
  @Retention(RUNTIME)
  @interface List {

    Insert[] value();

  }

}
