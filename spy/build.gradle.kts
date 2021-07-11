import com.impossibl.jdbc.spy.tools.SpyGen

plugins {
  `java-library`
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}

description = "PostgreSQL JDBC - NG - API Spy"


dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
}

val genDir = file("$buildDir/generated")
val gen11Dir = file("$buildDir/generated-java11")

sourceSets {
  main {
    java.srcDirs(genDir)
    java.include("com/impossibl/**")
  }
}
sourceSets.create("java11") {
  java.srcDir("src/main/java")
  java.srcDir(genDir)
  java.srcDir("src/main/java11")
}

tasks {

  val genTask = register("generator") {
    description = "Generate SPY relay, listener & trace classes"

    outputs.dir(genDir)

    doLast {
      genDir.mkdirs()
      SpyGen().generateTo(genDir)
    }
  }
  val gen11Task = register("generator11") {
    outputs.dir(gen11Dir)

    doLast {
      gen11Dir.mkdirs()
      SpyGen().generateTo(gen11Dir)
    }
  }

  compileJava {
    dependsOn(genTask)
    options.isDeprecation = true
  }

  val compileJavaTask = named("compileJava")
  named<JavaCompile>("compileJava11Java") {
    dependsOn(compileJavaTask)
    options.isDeprecation = true
  }

  javadoc {
    dependsOn(genTask)
  }

}

project.extra["moduleDescriptor"] = true

apply {
  from("$rootDir/shared/src/build/compile-java.gradle.kts")
  from("$rootDir/shared/src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
