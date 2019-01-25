import com.avast.gradle.dockercompose.ComposeExtension
import com.avast.gradle.dockercompose.DockerComposePlugin

buildscript {
  repositories { jcenter() }
  dependencies {
    classpath("com.avast.gradle:gradle-docker-compose-plugin:${Versions.dockerComposePlugin}")
  }
}

apply { type(DockerComposePlugin::class) }


val defaultPostgresVersion by extra("11")


val pgVersion = (project.properties["postgresVersions"] as? String ?: defaultPostgresVersion)
   .split(',')
   .map { it.trim() }
   .first()


val testTask = tasks.named<Test>("test") {
  useJUnitPlatform()
}


if ((project.properties["noDocker"] ?: false) == false) {

  configure<ComposeExtension> {
    useComposeFiles = listOf("src/test/docker/postgres-services.yml")
    startedServices = listOf("postgres")
    environment["PG_VERSION"] = pgVersion
    captureContainersOutputToFiles = file("$buildDir/test/containers")
    composeLogToFile = file("$buildDir/test/compose.log")
    projectName = "udt-test"
    isRequiredBy(testTask.get())
  }

  val compose = the<ComposeExtension>()

  testTask.configure {
    description = "Runs the unit tests against PostgreSQL $pgVersion"
    doFirst {
      val pgInfo = compose.servicesInfos["postgres"]!!.firstContainer
      systemProperty("pgjbdc.test.server", pgInfo.host)
      systemProperty("pgjdbc.test.port", pgInfo.ports[5432]!!)
    }
  }

}
