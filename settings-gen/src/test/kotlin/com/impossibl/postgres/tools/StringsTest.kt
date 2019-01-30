package com.impossibl.postgres.tools

import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test


class StringsTest {

  @Test
  fun testToAsciiDoc() {
    assertThat("<ul>\n  <li>Hello World</li>\n</ul>\n".toAsciiDoc(), equalTo("- Hello World"))
  }

}
