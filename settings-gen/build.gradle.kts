
plugins {
  `java-library`
  id("org.jetbrains.kotlin.jvm") version Versions.kotlinPlugin
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - Settings Processor"


dependencies {
  implementation("com.squareup:javapoet:${Versions.javaPoet}")
  implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
  testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testImplementation("com.google.testing.compile:compile-testing:${Versions.compilerTesting}")
}


tasks {

  test {
    useJUnitPlatform()
  }

}
