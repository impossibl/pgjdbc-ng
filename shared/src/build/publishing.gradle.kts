import java.net.URI

apply {
  plugin("maven-publish")
  plugin("signing")
}

val version: String by project
val isSnapshot: Boolean by project

val repositoryUrl =
   URI.create(
      if (isSnapshot)
        "https://oss.sonatype.org/content/repositories/snapshots/"
      else
        "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
   )!!

val organization: Map<String, Any> by project
val projectUrl: String = project.properties["url"] as String
val issuesUrl: String by project
val scmUrl: String by project
val scmGitUrl: String by project

configure<PublishingExtension> {

  publications {

    register<MavenPublication>("maven") {

      pom {
        name.set(project.description)

        url.set(projectUrl)

        organization {
          name.set(organization["name"] as String)
          url.set(organization["url"] as String)
        }

        issueManagement {
          system.set("GitHub")
          url.set(issuesUrl)
        }

        licenses {
          license {
            name.set("Apache License 2.0")
            distribution.set("repo")
          }
        }

        scm {
          url.set(projectUrl)
          connection.set(scmUrl)
          developerConnection.set(scmGitUrl)
        }

        developers {
          developer {
            id.set("kdubb0")
            name.set("Kevin Wooten")
          }
          developer {
            id.set("brettwooldridge")
            name.set("Brett Wooldridge")
          }
          developer {
            id.set("jesperpedersen")
            name.set("Jesper Pedersen")
          }
        }

      }

      from(components["java"])

      artifact(tasks.named<Jar>("sourcesJar").get())
      artifact(tasks.named<Jar>("javadocJar").get())

      tasks.findByName("uberJar")?.let {
        artifact(it)
      }

    }

  }

  repositories {
    maven {
      url = repositoryUrl
      credentials {
        username = project.findProperty("ossrhUsername") as? String ?: ""
        password = project.findProperty("ossrhPassword") as? String ?: ""
      }
    }
  }

}


configure<SigningExtension> {
  isRequired = !isSnapshot && gradle.taskGraph.hasTask("publishMavenPublicationToMavenRepository")
  sign(the<PublishingExtension>().publications["maven"])
}
