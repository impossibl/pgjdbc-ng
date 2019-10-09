
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.publish.maven.MavenPom
import java.net.URI

buildscript {
  repositories { gradlePluginPortal() }
  dependencies {
    classpath("com.github.jengelman.gradle.plugins:shadow:${Versions.shadowPlugin}")
  }
}

apply {
  plugin("maven-publish")
  plugin("signing")
}

val version: String by project
val isSnapshot: Boolean by project
val isRelease = (project.properties["release"]?.toString() ?: "false").toBoolean()

val repositoryUrl =
   URI.create(
      if (isRelease)
        "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
      else
        "https://oss.sonatype.org/content/repositories/snapshots/"
   )!!

val organization: Map<String, Any> by project
val projectUrl: String = project.properties["url"] as String
val issuesUrl: String by project
val scmUrl: String by project
val scmGitUrl: String by project

val pomCfg: MavenPom.() -> Unit = {

  name.set(project.name)
  description.set(project.description)

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

if (isSnapshot || isRelease) {

  configure<PublishingExtension> {

    publications {

      register<MavenPublication>("std") {

        pom(pomCfg)

        from(components["java"])

        artifact(tasks.named<Jar>("sourcesJar").get())
        artifact(tasks.named<Jar>("javadocJar").get())

      }

      tasks.findByPath("uberJar")?.let {

        register<MavenPublication>("shadow") {

          artifactId = "${artifactId}-all"

          pom(pomCfg)

          artifact(tasks.named<ShadowJar>("uberJar").get()) {
            classifier = ""
          }

          artifact(tasks.named<Jar>("sourcesJar").get())
          artifact(tasks.named<Jar>("javadocJar").get())
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

  if (isRelease) {

    configure<SigningExtension> {
      sign(the<PublishingExtension>().publications["maven"])
    }

  }

}
