package com.impossibl.postgres.test.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(PARAMETER)
@Retention(RUNTIME)
public @interface Random {
  int size() default 20;
  int origin() default 0;
  int bound() default Integer.MAX_VALUE;
  boolean codepoints() default false;
}
