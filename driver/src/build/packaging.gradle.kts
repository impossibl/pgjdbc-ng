
apply{ from("$rootDir/shared/src/build/uber-packaging.gradle.kts") }


tasks.named<ProcessResources>("processTestResources") {
  exclude("**/server/*.*")
}
