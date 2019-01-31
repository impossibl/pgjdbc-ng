
plugins {
}


allprojects {

  apply {
    plugin("idea")
  }

  group = "com.impossibl.pgjdbc-ng"
  version = "0.8-SNAPSHOT"

  extra["isSnapshot"] = version.toString().endsWith("SNAPSHOT")

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
