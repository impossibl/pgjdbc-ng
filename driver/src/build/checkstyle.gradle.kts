
apply {
  plugin("checkstyle")
}

dependencies {
  "checkstyle"("com.puppycrawl.tools:checkstyle:${Versions.checkstyle}")
}

configure<CheckstyleExtension> {
  val configDir = "$rootDir/config/checkstyle"
  configFile = file("$configDir/checkstyle.xml")
  configProperties = mapOf("configDir" to configDir)
}

tasks.named<Checkstyle>("checkstyleMain") { exclude("**/guava/**") }
tasks.named<Checkstyle>("checkstyleTest") { exclude("**/jdbc/shared/**") }
