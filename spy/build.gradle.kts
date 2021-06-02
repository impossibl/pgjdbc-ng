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

sourceSets {
  main {
    java.srcDirs(genDir)
  }
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

  compileJava {
    dependsOn(genTask)
    options.isDeprecation = true
  }

  javadoc {
    dependsOn(genTask)
  }

}

apply {
  from("$rootDir/shared/src/build/compile-java.gradle.kts")
  from("$rootDir/shared/src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
