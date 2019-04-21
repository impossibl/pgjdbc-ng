@file:Suppress("UnstableApiUsage")

buildCache {
  local<DirectoryBuildCache> {
    directory = rootDir.resolve("$rootDir/.build-cache")
    removeUnusedEntriesAfterDays = 30
  }
}
