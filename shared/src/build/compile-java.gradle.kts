
apply {
  plugin("java")
}


withConvention(JavaPluginConvention::class) {
  sourceCompatibility = JavaVersion.VERSION_1_8
  targetCompatibility = JavaVersion.VERSION_1_8
}
