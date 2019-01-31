
plugins {
  `java-library`
  id("org.jetbrains.kotlin.jvm") version Versions.kotlinPlugin
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - Settings Processor"


dependencies {
  compile("com.squareup:javapoet:${Versions.javaPoet}")
  compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
  testCompile("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testCompile("com.google.testing.compile:compile-testing:${Versions.compilerTesting}")
}


tasks {

  test {
    useJUnitPlatform()
  }

}
