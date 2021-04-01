
plugins {
  id("org.jetbrains.kotlin.jvm") version Versions.kotlinPlugin
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - UDT Generator"


dependencies {

  implementation(project(":pgjdbc-ng"))
  implementation("com.xenomachina:kotlin-argparser:${Versions.argParser}")
  implementation("com.squareup:javapoet:${Versions.javaPoet}")
  implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

  testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testImplementation("com.google.testing.compile:compile-testing:${Versions.compilerTesting}")
  
}


apply {
  from("src/build/testing.gradle.kts")
  from("src/build/uber-packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
