
plugins {
  `java-library`
  id("net.ltgt.apt-idea")
  id("com.adarshr.test-logger")
}

description = "PostgreSQL JDBC - NG - Driver"


val nettyVersion by extra("4.1.32.Final")
val junitOldVersion by extra("4.12")
val guavaTestVersion by extra("23.5-jre")
val junitVersion: String by project

dependencies {

  annotationProcessor(project(":settings-gen"))

  compile(project(":spy"))
  compile("io.netty:netty-common:$nettyVersion")
  compile("io.netty:netty-buffer:$nettyVersion")
  compile("io.netty:netty-transport:$nettyVersion")
  compile("io.netty:netty-codec:$nettyVersion")
  compile("io.netty:netty-handler:$nettyVersion")
  compile("io.netty:netty-transport-native-unix-common:$nettyVersion")
  compile("io.netty:netty-transport-native-kqueue:$nettyVersion")
  compile("io.netty:netty-transport-native-epoll:$nettyVersion")

  testCompile("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
  testCompile("org.junit.jupiter:junit-jupiter-params:$junitVersion")
  testCompile("org.junit.vintage:junit-vintage-engine:$junitVersion")
  testCompile("junit:junit:$junitOldVersion")
  testCompile("com.google.guava:guava:$guavaTestVersion")

}

apply {
  from("src/build/compile.gradle.kts")
  from("src/build/checkstyle.gradle.kts")
  from("src/build/testing.gradle.kts")
  from("src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}
