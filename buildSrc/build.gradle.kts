
plugins {
  id("java-library")
  id("org.jetbrains.kotlin.jvm") version "1.3.11"
}


repositories {
  mavenLocal()
  mavenCentral()
}


val javaPoetVersion = "1.11.1"
val javaParserVersion = "3.9.1"
val junitVersion = "5.3.2"
val compilerTesting = "0.15"


dependencies {

  compile("com.squareup:javapoet:$javaPoetVersion")
  compile("com.github.javaparser:javaparser-symbol-solver-core:$javaParserVersion")
  compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

  testCompile("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
  testCompile("com.google.testing.compile:compile-testing:$compilerTesting")
  
}


tasks.test {
  useJUnitPlatform()
}
