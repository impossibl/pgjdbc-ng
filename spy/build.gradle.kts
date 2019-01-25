import com.impossibl.jdbc.spy.tools.SpyGen

plugins {
  `java-library`
}

description = "PostgreSQL JDBC - NG - API Spy"


dependencies {
  testCompile("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
}


sourceSets {
  main {
    java.srcDirs("$buildDir/generated")
  }
}

tasks {

  val genTask = register("generator") {
    description = "Generate SPY relay, listener & trace classes"

    val genDir = file("$buildDir/generated")
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

}

apply {
  from("$rootDir/shared/src/build/compile-java.gradle.kts")
  from("$rootDir/shared/src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
