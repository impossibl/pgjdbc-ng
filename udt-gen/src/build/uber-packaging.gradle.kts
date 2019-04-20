import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
  repositories { gradlePluginPortal() }
  dependencies {
    classpath("com.github.jengelman.gradle.plugins:shadow:${Versions.shadowPlugin}")
  }
}


apply { from("$rootDir/shared/src/build/uber-packaging.gradle.kts") }


/**
 * UBER JAR
 */

tasks.named<ShadowJar>("uberJar") {
  manifest {
    from()
    attributes(mapOf(
       "Main-Class" to "com.impossibl.postgres.tools.UDTGenerator"
    ))
  }
}
