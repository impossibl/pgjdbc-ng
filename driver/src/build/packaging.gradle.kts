
apply{ from("$rootDir/shared/src/build/uber-packaging.gradle.kts") }


tasks.named<ProcessResources>("processTestResources") {
  exclude("**/server/*.*")
}

configurations {
  create("docs")
}

val docsTask = tasks.register<Tar>("docs") {
  compression = Compression.GZIP

  archiveClassifier.set("docs")

  from("$buildDir/generated/docs")

  dependsOn(tasks.named("classes"))
}

artifacts {
  add("docs", docsTask)
}
