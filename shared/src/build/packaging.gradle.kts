
val mainSourceSet = the<SourceSetContainer>()["main"]!!

/**
 * STD JAR
 */

val jar = tasks.named<Jar>("jar") {

  val organization: Map<String, Any> by project
  val url: String by project

  // Prefix "sub" modules with project name
  if (archiveBaseName.orNull != "pgjdbc-ng") {
    archiveBaseName.set("pgjdbc-ng-${archiveBaseName.get()}")
  }

  manifest {
    attributes(
       "Implementation-Title" to project.description,
       "Implementation-Version" to project.version,
       "Implementation-Vendor-Id" to project.group,
       "Implementation-Vendor" to organization["name"],
       "Implementation-URL" to url,
       "Created-By" to "${System.getProperty("java.version")} (${System.getProperty("java.vendor")})"
    )
  }
  if (project.extra["moduleDescriptor"] as Boolean) {
    manifest {
      attributes("Multi-Release" to "true")
    }
    into("META-INF/versions/11") {
      from(project.the<SourceSetContainer>()["java11"].output)
      include("module-info.class")
    }
  }
}



/**
 * SOURCES
 */

tasks.register<Jar>("sourcesJar") {
  dependsOn(tasks.named("classes"))
  archiveClassifier.set("sources")
  from(mainSourceSet.allSource)
}



/**
 * JAVADOC
 */

val javadoc = tasks.named<Javadoc>("javadoc") {
  options {
    title = "${project.name} $version"
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
  isFailOnError = false
}

tasks.register<Jar>("javadocJar") {
  dependsOn(tasks.named("javadoc"))
  archiveClassifier.set("javadoc")
  from(javadoc.get().destinationDir)
}
