package com.impossibl.jdbc.spy

import org.junit.Test
import java.io.File

class SpyGenTest {

  @Test
  fun testGen() {
    SpyGen().generate(File("target/test-output"))
  }

}
