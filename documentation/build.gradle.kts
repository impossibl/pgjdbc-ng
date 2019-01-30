plugins {
  id("org.asciidoctor.convert") version Versions.asciiDoctorPlugin
  id("org.ajoberstar.git-publish") version Versions.gitPublishPlugin
}

val javadocs = configurations.create("javadocs")
val docs = configurations.create("docs")

dependencies {

  docs(project(":pgjdbc-ng", "docs"))
  javadocs(project(":pgjdbc-ng"))
}

tasks {


  val aggregateJavadocs = register<Javadoc>("aggregateJavadocs") {
    destinationDir = file("$buildDir/javadoc")
    options {
      title = "${project.name} $version API"
    }

    rootProject.subprojects.filter { project != it }.forEach { project ->

      project.tasks.withType(Javadoc::class.java).forEach { task ->
        dependsOn(task)
        source += task.source
        classpath += task.classpath
        excludes += task.excludes
        includes += task.includes
      }
    }
  }



  val collectDocs = register<Sync>("collectDocs") {
    from(docs)
    from(docs.map { tarTree(it) })
    into("$buildDir/tmp/docs")
  }

  asciidoctor {

    val docsDir = "$projectDir/src/docs/asciidoc"
    val examplesDir = "$projectDir/src/docs/examples"
    val includeDocsDir = "$buildDir/tmp/docs"
    val docinfosDir = "$docsDir/docinfos"

    sourceDir = file(docsDir)
    sources(delegateClosureOf<PatternSet> {
      include("**/index.adoc")
    })

    outputDir = file("$buildDir/docs")

    setBackends("html5")

    inputs.dir(examplesDir)
    inputs.dir(includeDocsDir)
    inputs.dir(docinfosDir)

    attributes(mapOf(
       "toc" to "left",
       "incdir" to includeDocsDir,
       "exdir" to examplesDir,
       "docinfodir" to docinfosDir,
       "icons" to "font",
       "driverdepgroup" to rootProject.project(":pgjdbc-ng").group.toString(),
       "driverdepname" to rootProject.project(":pgjdbc-ng").name,
       "driverdepver" to version,
       "udtdepname" to rootProject.project(":udt-gen").name,
       "driverdepclass" to "all",
       "driverdeprepo" to if (version.toString().endsWith("SNAPSHOT")) "snapshots" else "releases",
       "maintainers" to loadMaintainers(docsDir)
    ))

    dependsOn(collectDocs)
  }

  gitPublish {

    repoUri.set("git@github.com:impossibl/pgjdbc-ng.git")
    branch.set("gh-pages")

    contents {
      if (version.toString().endsWith("SNAPSHOT")) {
        from("$buildDir/docs/html5") {
          into("docs/snapshot")
        }
        from ("$buildDir/javadoc") {
          into("docs/snapshot/javadoc")
        }
      }
      else {
        from("$buildDir/docs/html5") {
          into("docs/$version")
        }
        from ("$buildDir/javadoc") {
          into("docs/$version/javadoc")
        }
        from("$buildDir/docs/html5") {
          into("docs/current")
        }
        from ("$buildDir/javadoc") {
          into("docs/current/javadoc")
        }
      }
    }

    preserve { include("**/*") }
  }

  build {
    dependsOn(aggregateJavadocs)
    dependsOn(asciidoctor)
  }

  gitPublishPush.configure {
    dependsOn(build)
  }

}


fun loadMaintainers(docsDir: String): List<String> =
   File("$docsDir/maintainers.txt").readLines()
