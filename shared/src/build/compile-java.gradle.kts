
configure<JavaPluginExtension> {
  toolchain {
    sourceCompatibility = Versions.javaTarget
    targetCompatibility = Versions.javaTarget
  }
}

tasks.named<JavaCompile>("compileJava") {
  val targetVersion: Int = Integer.parseInt(Versions.javaTarget.majorVersion)
  options.release.set(targetVersion)
}

if (project.extra["moduleDescriptor"] as Boolean) {
  tasks.named<JavaCompile>("compileJava11Java") {
    options.release.set(11)
    options.javaModuleVersion.set(project.version as String)
  }

  configurations["java11CompileClasspath"].extendsFrom(configurations["compileClasspath"])
}
