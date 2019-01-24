
val mainSourceSet = the<SourceSetContainer>()["main"]!!

/**
 * STD JAR
 */

val jar = tasks.named<Jar>("jar") {

  val organization: Map<String, Any> by project
  val url: String by project

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
  options.quiet()
  // options.addStringOption("Xdoclint:none", "-quiet")
  isFailOnError = false
}

tasks.register<Jar>("javadocJar") {
  dependsOn(tasks.named("javadoc"))
  archiveClassifier.set("javadoc")
  from(javadoc.get().destinationDir)
}
