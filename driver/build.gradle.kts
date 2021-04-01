
plugins {
  `java-library`
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}

description = "PostgreSQL JDBC - NG - Driver"


dependencies {

  annotationProcessor(project(":settings-gen"))

  implementation(project(":spy"))
  implementation("io.netty:netty-common:${Versions.netty}")
  implementation("io.netty:netty-buffer:${Versions.netty}")
  implementation("io.netty:netty-transport:${Versions.netty}")
  implementation("io.netty:netty-codec:${Versions.netty}")
  implementation("io.netty:netty-handler:${Versions.netty}")
  implementation("io.netty:netty-transport-native-unix-common:${Versions.netty}")
  implementation("io.netty:netty-transport-native-kqueue:${Versions.netty}")
  implementation("io.netty:netty-transport-native-epoll:${Versions.netty}")

  testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testImplementation("org.junit.jupiter:junit-jupiter-params:${Versions.junit}")
  testImplementation("org.junit.vintage:junit-vintage-engine:${Versions.junit}")
  testImplementation("junit:junit:${Versions.junitClassic}")
  testImplementation("com.google.guava:guava:${Versions.guava}")

}

apply {
  from("src/build/compile.gradle.kts")
  from("src/build/checkstyle.gradle.kts")
  from("src/build/testing.gradle.kts")
  from("src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}


tasks {
  compileJava {
    outputs.dir("$buildDir/generated/docs")
  }
  processResources {
    expand(project.properties)
  }
}
