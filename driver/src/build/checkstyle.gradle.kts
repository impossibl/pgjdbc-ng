
apply {
  plugin("checkstyle")
}

val checkstyleVersion: String by project

dependencies {
  "checkstyle"("com.puppycrawl.tools:checkstyle:$checkstyleVersion")
}

configure<CheckstyleExtension> {
  val configDir = "$rootDir/config/checkstyle"
  configFile = file("$configDir/checkstyle.xml")
  configProperties = mapOf("configDir" to configDir)
}

tasks.named<Checkstyle>("checkstyleMain") { exclude("**/guava/**") }
tasks.named<Checkstyle>("checkstyleTest") { exclude("**/jdbc/shared/**") }
