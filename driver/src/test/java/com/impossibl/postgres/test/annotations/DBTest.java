package com.impossibl.postgres.test.annotations;


import com.impossibl.postgres.test.extensions.DBProvider;
import com.impossibl.postgres.test.extensions.ExtensionInstalledCondition;
import com.impossibl.postgres.test.extensions.RandomProvider;
import com.impossibl.postgres.test.extensions.SchemaManager;
import com.impossibl.postgres.test.extensions.UpdateManager;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DBProvider.class)
@ExtendWith(SchemaManager.class)
@ExtendWith(UpdateManager.class)
@ExtendWith(RandomProvider.class)
@ExtendWith(ExtensionInstalledCondition.class)
@Target(TYPE)
@Retention(RUNTIME)
public @interface DBTest {

  String host() default "localhost";
  String db() default "test";
  String user() default "test";
  String password() default "test";

}
