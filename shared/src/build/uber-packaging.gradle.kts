import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
  repositories { gradlePluginPortal() }
  dependencies {
    classpath("com.github.jengelman.gradle.plugins:shadow:${Versions.shadowPlugin}")
  }
}


apply { from("$rootDir/shared/src/build/packaging.gradle.kts") }


/**
 * UBER JAR
 */

val jar by tasks.existing(Jar::class)
val mainSourceSet = the<SourceSetContainer>()["main"]!!

tasks.register<ShadowJar>("uberJar") {
  manifest {
    from(jar.get().manifest)
  }
  archiveClassifier.set("all")
  from(mainSourceSet.output)
  configurations = listOf(project.configurations["runtime"])
  relocate( "io.netty", "com.impossibl.shadow.io.netty")
  minimize()
}
