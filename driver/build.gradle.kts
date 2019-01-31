
plugins {
  `java-library`
  id("net.ltgt.apt-idea") version Versions.aptIdeaPlugin
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}

description = "PostgreSQL JDBC - NG - Driver"


dependencies {

  annotationProcessor(project(":settings-gen"))

  compile(project(":spy"))
  compile("io.netty:netty-common:${Versions.netty}")
  compile("io.netty:netty-buffer:${Versions.netty}")
  compile("io.netty:netty-transport:${Versions.netty}")
  compile("io.netty:netty-codec:${Versions.netty}")
  compile("io.netty:netty-handler:${Versions.netty}")
  compile("io.netty:netty-transport-native-unix-common:${Versions.netty}")
  compile("io.netty:netty-transport-native-kqueue:${Versions.netty}")
  compile("io.netty:netty-transport-native-epoll:${Versions.netty}")

  testCompile("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
  testCompile("org.junit.jupiter:junit-jupiter-params:${Versions.junit}")
  testCompile("org.junit.vintage:junit-vintage-engine:${Versions.junit}")
  testCompile("junit:junit:${Versions.junitClassic}")
  testCompile("com.google.guava:guava:${Versions.guava}")

}

apply {
  from("src/build/compile.gradle.kts")
  from("src/build/checkstyle.gradle.kts")
  from("src/build/testing.gradle.kts")
  from("src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
