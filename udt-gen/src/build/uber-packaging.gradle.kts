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
  relocate("com.xenomachina", "com.impossibl.shadow.com.xenomachina")
  relocate("com.squareup", "com.impossibl.shadow.com.squareup")
  relocate("org.jetbrains.kotlin", "com.impossibl.shadow.org.jetbrains.kotlin")
  minimize()
  manifest {
    from()
    attributes(mapOf(
       "Main-Class" to "com.impossibl.postgres.tools.UDTGenerator"
    ))
  }
}
