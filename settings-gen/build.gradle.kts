
plugins {
  `java-library`
  id("org.jetbrains.kotlin.jvm")
}


group = "com.impossibl.pgjdbc-ng.tools"
description = "PostgreSQL JDBC - NG - Settings Processor"


val javaPoetVersion: String by project
val junitVersion: String by project
val compilerTestingVersion: String by project


dependencies {
  compile("com.squareup:javapoet:$javaPoetVersion")
  compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
  testCompile("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
  testCompile("com.google.testing.compile:compile-testing:$compilerTestingVersion")
}


tasks {

  test {
    useJUnitPlatform()
  }

}
