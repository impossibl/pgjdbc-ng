plugins {
  `java-library`
  id("com.adarshr.test-logger") version Versions.testLoggerPlugin
}

description = "PostgreSQL JDBC - NG - Driver"

sourceSets.create("java11") {
  java.srcDir("src/main/java")
  java.srcDir("src/main/java11")
  java.srcDir("$buildDir/generated/sources/annotationProcessor/java/java11")
}

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

project.extra["moduleDescriptor"] = true

apply {
  from("src/build/compile.gradle.kts")
  from("src/build/checkstyle.gradle.kts")
  from("src/build/testing.gradle.kts")
  from("src/build/packaging.gradle.kts")
  from("$rootDir/shared/src/build/publishing.gradle.kts")
}

// Gradle has a bug which prevents using --processor-module-path
// So, we need to re-use the generated sources from compileJava
tasks.create<Copy>("generate-java11-sources") {
  from("$buildDir/generated/sources/annotationProcessor/java/main")
  into("$buildDir/generated/sources/annotationProcessor/java/java11")
  class Filter : Transformer<String, String> {

    override fun transform(line: String): String {
      return line.replace("javax.annotation.Generated", "javax.annotation.processing.Generated")
    }
  }
  filter(Filter())
}

tasks {
  compileJava {
    outputs.dir("$buildDir/generated/docs")
  }
  named("generate-java11-sources") {
    dependsOn(project.tasks.named("compileJava"))
  }
  named<JavaCompile>("compileJava11Java") {
    dependsOn(project.tasks.named("generate-java11-sources"))
  }
  classes {
    dependsOn(project.tasks.named("compileJava11Java"))
  }
  processResources {
    expand(project.properties)
  }
}
