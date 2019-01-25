
plugins {
  id("org.jetbrains.kotlin.jvm") version "1.3.11" apply false
  id("net.ltgt.apt-idea") version "0.20" apply false
  id("com.adarshr.test-logger") version "1.6.0" apply false
}


allprojects {

  apply {
    plugin("idea")
  }

  group = "com.impossibl.pgjdbc-ng"
  version = "0.8-SNAPSHOT"

  val organization by extra(mapOf(
     "name" to "impossibl.com",
     "url" to "https://github.com"
  ))

  val url by extra("${organization["url"]}/pgjdbc-ng")
  extra["issuesUrl"] = "$url/issues"
  extra["scmUrl"] = "scm:$url.git"
  extra["scmGitUrl"] = "scm:git@github.com:impossibl/pgjdbc-ng.git"
}

subprojects {

  repositories {
    mavenLocal()
    mavenCentral()
  }

}
