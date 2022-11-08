
apply {
  from("$rootDir/shared/src/build/compile-java.gradle.kts")
}

tasks.named<JavaCompile>("compileJava") {
  options.compilerArgs.add("-Adoc.dir=$buildDir/generated/docs/")
  options.isDeprecation = true
  options.annotationProcessorGeneratedSourcesDirectory = file("$buildDir/generated/sources/annotationProcessor/java/main")
}

tasks.named<JavaCompile>("compileJava11Java") {
  options.compilerArgs.add("-Adoc.dir=$buildDir/generated/docs-java11/")
  options.isDeprecation = true
  options.annotationProcessorGeneratedSourcesDirectory = file("$buildDir/generated/sources/annotationProcessor/java/java11")
}

tasks.named<JavaCompile>("compileTestJava") {
  options.compilerArgs.add("-parameters")
  options.isDeprecation = true
}
