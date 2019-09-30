plugins {
  java
  application
}

description = "PostgreSQL JDBC - NG - Test Tools"

repositories {
  mavenCentral()
  maven("https://oss.sonatype.org/content/repositories/snapshots")
}

dependencies {

  implementation(project(":pgjdbc-ng"))
  implementation("com.github.jsqlparser:jsqlparser:1.3")
  implementation("org.postgresql:postgresql:42.2.5")

}

application {
  mainClassName = "com.impossibl.postgres.tools.Replay"
}
