
apply {
  plugin("java")
}


withConvention(JavaPluginConvention::class) {
  sourceCompatibility = Versions.javaTarget
  targetCompatibility = Versions.javaTarget
}
