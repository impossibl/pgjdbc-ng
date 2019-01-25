import org.gradle.api.JavaVersion

object Versions {

  /**
   * Source
   */
  val javaTarget = JavaVersion.VERSION_1_8
  const val kotlin = "1.3.20"

  /**
   * Required Libraries
   */
  const val netty = "4.1.32.Final"

  /**
   * Tooling Libraries
   */
  const val javaPoet = "1.11.1"
  const val argParser = "2.0.7"

  /**
   * Testing dependencies
   */
  const val junit = "5.3.2"
  const val junitClassic = "4.12"
  const val guava = "23.5-jre"
  const val compilerTesting = "0.15"
  const val checkstyle = "6.18"

  /**
   * Plugin dependencies
   */
  const val shadowPlugin = "4.0.4"
  const val dockerComposePlugin = "0.8.13"

}
