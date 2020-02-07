@file:Suppress("UnstableApiUsage")

buildCache {
  local {
    directory = rootDir.resolve("$rootDir/.build-cache")
    removeUnusedEntriesAfterDays = 30
  }
}
