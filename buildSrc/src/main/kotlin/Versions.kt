import org.gradle.api.JavaVersion

object Versions {

  /**
   * Source
   */
  val javaTarget = JavaVersion.VERSION_1_8
  const val kotlin = "1.4.31"

  /**
   * Required Libraries
   */
  const val netty = "4.1.63.Final"

  /**
   * Tooling Libraries
   */
  const val javaPoet = "1.13.0"
  const val argParser = "2.0.7"
  const val asciidoctorJ = "2.4.3"

  /**
   * Testing dependencies
   */
  const val junit = "5.7.1"
  const val junitClassic = "4.12"
  const val guava = "23.5-jre"
  const val compilerTesting = "0.19"
  const val checkstyle = "6.18"

  /**
   * Plugin dependencies
   */
  const val kotlinPlugin = kotlin
  const val shadowPlugin = "7.0.0"
  const val dockerComposePlugin = "0.14.2"
  const val asciiDoctorPlugin = "3.2.0"
  const val gitPublishPlugin = "3.0.0"
  const val testLoggerPlugin = "3.0.0"
  const val githubReleasePlugin = "2.2.12"

}
