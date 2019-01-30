package com.impossibl.postgres.tools

import java.util.regex.Pattern


val acronyms = setOf("i/o", "sql", "ssl", "url", "ca", "api")
val abbreviations = mapOf(
    "recv" to "receive",
    "io" to "i/o"
)

fun String.toTitleCase(): String {
  return this.split(".", "-")
     .map { abbreviations.getOrDefault(it, it) }
     .joinToString(" ") { if (acronyms.contains(it)) it.toUpperCase() else it.capitalize() }
}

fun String.replaceTag(tag: String, transform: (MatchResult) -> String): String {
  val tagr = """<$tag>((.|\n)*?)</$tag>""".toRegex(RegexOption.MULTILINE)
  return this.replace(tagr, transform)
}

fun String.rewriteAnchorsAsLinks(): String {
  val tagr = """<a href="(.*)">((.|\n)*?)</a>""".toRegex(RegexOption.MULTILINE)
  return this.replace(tagr) { res -> "${res.groupValues[1]}[${res.groupValues[2]}]" }
}

fun String.toAsciiDoc(): String {
  return replaceTag("ul") { res -> "\n" + res.groupValues[1].replaceTag("li") { res2 -> "- ${res2.groupValues[1]}" } }
     .replace("""\n[ \t]*-(\s*\n)?""".toRegex(), "\n- ")
     .replaceTag("ol") { res -> "\n" + res.groupValues[1].replaceTag("li") { res2 -> ". ${res2.groupValues[1]}" } }
     .replace("""\n[ \t]*\.(\s*\n)?""".toRegex(), "\n- ")
     .replaceTag("code") { res -> "`${res.groupValues[1]}`" }
     .replaceTag("i") { res -> "_${res.groupValues[1]}_" }
     .replaceTag("b") { res -> "*${res.groupValues[1]}*" }
     .rewriteAnchorsAsLinks()
     .replace("""\n[ \t]*""".toRegex(), "\n")
}

fun String.escapeAsciiDoc(): String =
   when (this.first()) {
     '*', '#', '-', '|' -> "{empty}${this}"
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

fun String.markdownAnchor(): String {
  return this.split("""[^\w]""".toRegex()).joinToString("-").toLowerCase()
}
