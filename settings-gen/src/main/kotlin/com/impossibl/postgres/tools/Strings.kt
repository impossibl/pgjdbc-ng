package com.impossibl.postgres.tools

import java.util.regex.Pattern


val acronyms = setOf("i/o", "sql", "ssl", "url", "ca")
val abbreviations = mapOf(
    "recv" to "receive",
    "io" to "i/o"
)

fun String.toTitleCase(): String {
  return this.split(".", "-")
     .map { abbreviations.getOrDefault(it, it) }
     .joinToString(" ") { if (acronyms.contains(it)) it.toUpperCase() else it.capitalize() }
}

fun String.escapeMarkdown(): String =
   when (this.first()) {
     '*', '#', '`', '@' -> "\\${this}"
     else -> this
   }

fun String.toUpperSnakeCase(): String =
   this.replace('.', '_').replace('-', '_').toUpperCase()

fun String.escapePoet(): String =
   this.replace("$", "$$")

val String.beanPropertyName: String get() {
  val parts = split("-", ".")
  return (parts.take(1) + parts.drop(1).map { it.capitalize() }).joinToString("")
}


fun String.dashesFromSnakeCase(): String {
  return this.toLowerCase().replace('_', '-')
}

private val CAPITALIZED_WORDS_PATTERN = Pattern.compile("[A-Z][^A-Z]*")

fun String.dashesFromCamelCase(): String {

  val newVal = StringBuilder()
  var first = true

  val matcher = CAPITALIZED_WORDS_PATTERN.matcher(this)
  while (matcher.find()) {
    val group = matcher.group(0)
    if (first) {
      // Handle "lowerCamelCase" first word
      if (matcher.start() != 0) {
        newVal.append(this, 0, matcher.start())
        newVal.append('-')
      }
    }
    else {
      newVal.append('-')
    }
    newVal.append(group.toLowerCase())
    first = false
  }

  return newVal.toString()
}
