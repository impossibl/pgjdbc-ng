import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
  repositories { gradlePluginPortal() }
  dependencies {
    classpath("gradle.plugin.com.github.jengelman.gradle.plugins:shadow:${Versions.shadowPlugin}")
  }
}


apply { from("$rootDir/shared/src/build/uber-packaging.gradle.kts") }


/**
 * UBER JAR
 */

tasks.named<ShadowJar>("uberJar") {
  relocate("io.netty", "com.impossibl.shadow.io.netty")
  minimize()
}
