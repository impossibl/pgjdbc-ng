
plugins {
  id("org.jetbrains.kotlin.jvm")
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - UDT Generator"


dependencies {

  compile(project(":pgjdbc-ng"))
  compile("com.xenomachina:kotlin-argparser:${Versions.argParser}")
  compile("com.squareup:javapoet:${Versions.javaPoet}")
  compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

  testCompile("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testCompile("com.google.testing.compile:compile-testing:${Versions.compilerTesting}")
  
}


apply {
  from("src/build/testing.gradle.kts")
  from("$rootDir/shared/src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
