
import com.github.breadmoirai.githubreleaseplugin.GithubReleaseTask
import de.undercouch.gradle.tasks.download.Download

plugins {
  base
  id("de.undercouch.download") version "3.4.3"
  id("com.github.breadmoirai.github-release") version Versions.githubReleasePlugin
}


allprojects {

  apply {
    plugin("idea")
  }

  group = "com.impossibl.pgjdbc-ng"
  version = "0.8.6"

  extra["isSnapshot"] = version.toString().endsWith("SNAPSHOT")

  val organization by extra(mapOf(
     "name" to "impossibl.com",
     "url" to "https://github.com"
  ))

  val url by extra("${organization["url"]}/pgjdbc-ng")
  extra["issuesUrl"] = "$url/issues"
  extra["scmUrl"] = "scm:$url.git"
  extra["scmGitUrl"] = "scm:git@github.com:impossibl/pgjdbc-ng.git"
}

subprojects {

  repositories {
    mavenLocal()
    mavenCentral()
  }

}

val isSnapshot: Boolean by project

tasks {

  val downloadTasks = listOf(
     centralDownload("com.impossibl.pgjdbc-ng", "pgjdbc-ng-all"),
     centralDownload("com.impossibl.pgjdbc-ng", "pgjdbc-ng", "javadoc"),
     centralDownload("com.impossibl.pgjdbc-ng", "pgjdbc-ng", "sources"),
     centralDownload("com.impossibl.pgjdbc-ng", "spy"),
     centralDownload("com.impossibl.pgjdbc-ng", "spy", "javadoc"),
     centralDownload("com.impossibl.pgjdbc-ng", "spy", "sources"),
     centralDownload("com.impossibl.pgjdbc-ng.tools", "udt-gen-all"),
     centralDownload("com.impossibl.pgjdbc-ng.tools", "udt-gen", "javadoc"),
     centralDownload("com.impossibl.pgjdbc-ng.tools", "udt-gen", "sources")
  )

  val downloadArtifacts = register<Task>("downloadArtifacts") {
    outputs.files(downloadTasks.flatMap { it.get().outputFiles })
    dependsOn(downloadTasks)
  }

  named<GithubReleaseTask>("githubRelease") {
    dependsOn(downloadArtifacts)
    setAuthorization("token ${project.properties["github.token"]?.toString() ?: ""}")
    setOwner("impossibl")
    setRepo("pgjdbc-ng")
    setTagName("v$version")
    setTargetCommitish("develop")
    setDraft(true)
    setPrerelease(isSnapshot)
    setOverwrite(true)
    setBody(
       """
       ## [Release Notes](https://impossibl.github.io/pgjdbc-ng/docs/$version/release-notes)

       ## [User Guide](https://impossibl.github.io/pgjdbc-ng/docs/$version/user-guide)
     """.trimIndent().trim()
    )
    releaseAssets.from(downloadArtifacts)
  }

}

fun centralDownload(group: String, artifact: String, classifier: String? = null): TaskProvider<Download> {
  val base = "http://oss.sonatype.org/service/local/artifact/maven/redirect"
  val repo = if(isSnapshot) "snapshots" else "releases"
  val queryClassifier = classifier?.let { "&c=$it" } ?: ""
  val dest = "$buildDir/artifacts/$artifact-$version${classifier?.let { "-$it" } ?: ""}.jar"

  tasks.named("clean") {
    doFirst {
      file(dest).delete()
    }
  }

  return tasks.register<Download>("downloadCentral" + "$group-$artifact-${classifier ?: ""}") {
    outputFiles.add(file(dest))
    src("$base?r=$repo&g=$group&a=$artifact$queryClassifier&v=$version")
    dest(dest)
  }

}
