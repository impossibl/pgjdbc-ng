
plugins {
  id("java-library")
  id("org.jetbrains.kotlin.jvm") version "1.4.20"
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

  implementation("com.squareup:javapoet:$javaPoetVersion")
  implementation("com.github.javaparser:javaparser-symbol-solver-core:$javaParserVersion")
  implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

  testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
  testImplementation("com.google.testing.compile:compile-testing:$compilerTesting")
  
}


tasks.test {
  useJUnitPlatform()
}
