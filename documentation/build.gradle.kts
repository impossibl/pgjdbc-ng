plugins {
  id("org.asciidoctor.jvm.convert") version Versions.asciiDoctorPlugin
  id("org.ajoberstar.git-publish") version Versions.gitPublishPlugin
}

val isSnapshot: Boolean by project

val docsRepoUri = project.properties.getOrDefault("docsRepoUri", "git@github.com:impossibl/pgjdbc-ng.git").toString()

val javadocs: Configuration = configurations.create("javadocs")
val docs: Configuration = configurations.create("docs")

dependencies {

  docs(project(":pgjdbc-ng", "docs"))
  javadocs(project(":pgjdbc-ng"))
}

tasks {


  val aggregateJavadocs = register<Javadoc>("aggregateJavadocs") {
    setDestinationDir(file("$buildDir/javadoc"))
    options {
      title = "PGJDBC-NG $version"
      encoding = "UTF-8"
      (this as StandardJavadocDocletOptions).apply {
        addBooleanOption("Xdoclint:none", true)
        if (JavaVersion.current().isJava9Compatible) {
          addBooleanOption("html5", true)
        }
        source("8")
        links("https://docs.oracle.com/javase/8/docs/api/")
        use(true)
        noTimestamp(true)
      }
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

  asciidoctorj {
    setVersion(Versions.asciidoctorJ)
  }

  asciidoctor {

    val docsDir = "$projectDir/src/docs/asciidoc"
    val examplesDir = "$projectDir/src/docs/examples"
    val includeDocsDir = "$buildDir/tmp/docs"
    val docinfosDir = "$docsDir/docinfos"

    setSourceDir(file(docsDir))
    sources(delegateClosureOf<PatternSet> {
      include("**/index.adoc")
    })

    setOutputDir(file("$buildDir/docs/html5"))
    outputOptions {
      backends("html5")
    }

    baseDirFollowsSourceFile()

    inputs.dir(examplesDir)
    inputs.dir(includeDocsDir)
    inputs.dir(docinfosDir)

    val driverPrj = rootProject.project(":pgjdbc-ng")
    val spyPrj = rootProject.project(":spy")
    val udtPrj = rootProject.project(":udt-gen")

    attributes(mapOf(
       "toc" to "left",
       "incdir" to includeDocsDir,
       "exdir" to examplesDir,
       "docinfodir" to docinfosDir,
       "icons" to "font",
       "revnumber" to version,
       "driverdepgroup" to driverPrj.group.toString(),
       "driverdepname" to driverPrj.name,
       "driverdepver" to driverPrj.version,
       "spydepgroup" to spyPrj.group.toString(),
       "spydepname" to spyPrj.name,
       "spydepver" to spyPrj.version,
       "udtdepgroup" to udtPrj.group.toString(),
       "udtdepname" to udtPrj.name,
       "udtdepver" to udtPrj.version,
       "ubersuffix" to "all",
       "mavenrepo" to if (isSnapshot) "snapshots" else "releases",
       "maintainers" to loadMaintainers(docsDir),
       "source-highlighter" to "coderay",
       "favicon" to "../../../images/ng-logo.png"
    ))

    dependsOn(collectDocs)
  }

  gitPublish {

    repoUri.set(docsRepoUri)
    branch.set("gh-pages")

    contents {
      from("$buildDir/docs/html5") {
        into("docs/$version")
      }
      from ("$buildDir/javadoc") {
        into("docs/$version/javadoc")
      }
      if (!isSnapshot) {
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

  gitPublishPush {
    dependsOn(build)
  }

}


fun loadMaintainers(docsDir: String): List<String> =
   File("$docsDir/maintainers.txt").readLines()
