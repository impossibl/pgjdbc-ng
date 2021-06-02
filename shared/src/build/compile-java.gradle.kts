
configure<JavaPluginExtension> {
  toolchain {
    sourceCompatibility = Versions.javaTarget
    targetCompatibility = Versions.javaTarget
  }
}

val javaToolchains = extensions.getByName("javaToolchains") as JavaToolchainService

tasks.withType<JavaCompile>().configureEach {
  javaCompiler.set(javaToolchains.compilerFor {
    languageVersion.set(JavaLanguageVersion.of(Versions.javaTarget.majorVersion))
  })
}
