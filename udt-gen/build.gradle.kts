
plugins {
  id("org.jetbrains.kotlin.jvm")
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - UDT Generator"


val argParserVersion by extra("2.0.7")
val javaPoetVersion: String by project
val junitVersion: String by project
val compilerTestingVersion: String by project


dependencies {
  compile(project(":pgjdbc-ng"))
  compile("com.xenomachina:kotlin-argparser:$argParserVersion")
  compile("com.squareup:javapoet:$javaPoetVersion")
  compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
  testCompile("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
  testCompile("com.google.testing.compile:compile-testing:$compilerTestingVersion")
}


apply {
  from("src/build/testing.gradle.kts")
  from("$rootDir/shared/src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
