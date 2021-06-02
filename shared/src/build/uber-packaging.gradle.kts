import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
  repositories { gradlePluginPortal() }
  dependencies {
    classpath("gradle.plugin.com.github.jengelman.gradle.plugins:shadow:${Versions.shadowPlugin}")
  }
}


apply { from("$rootDir/shared/src/build/packaging.gradle.kts") }


/**
 * UBER JAR
 */

val jar by tasks.existing(Jar::class)
val mainSourceSet = the<SourceSetContainer>()["main"]!!

tasks.register<ShadowJar>("uberJar") {
  archiveAppendix.set("all")
  manifest {
    from(jar.get().manifest)
  }
  from(mainSourceSet.output)
  configurations = listOf(project.configurations["runtimeClasspath"])
}
