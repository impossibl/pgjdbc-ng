plugins {
  id("org.asciidoctor.convert") version Versions.asciiDoctorPlugin
  id("org.ajoberstar.git-publish") version Versions.gitPublishPlugin
}

val docs = configurations.create("docs")

dependencies {
  docs(project(":pgjdbc-ng", "docs"))
}

tasks {

  val collect = register<Sync>("collect") {
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

    dependsOn(collect)
  }

  gitPublish {

    repoUri.set("git@github.com:impossibl/pgjdbc-ng.git")
    branch.set("gh-pages")

    contents {
      if (version.toString().endsWith("SNAPSHOT")) {
        from("$buildDir/docs/html5") {
          into("docs/snapshot")
        }
      }
      else {
        from("$buildDir/docs/html5") {
          into("docs/$version")
        }
        from("$buildDir/docs/html5") {
          into("docs/current")
        }
      }
    }

    preserve { include("**/*") }
  }

  build {
    dependsOn(asciidoctor)
  }

}


fun loadMaintainers(docsDir: String): List<String> =
   File("$docsDir/maintainers.txt").readLines()
