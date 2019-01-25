package com.impossibl.postgres.tools

import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler.javac
import com.google.testing.compile.JavaFileObjects
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test


class SettingsProcessorTest {

  @Test
  @Disabled
  fun testCompile() {

    val result = javac()
       .withProcessors(SettingsProcessor())
       .compile(JavaFileObjects.forResource("MySettingsTest.java"))

    assertThat(result).succeeded()
    assertThat(result)
       .generatedSourceFile("com.impossibl.postgres.jdbc.AbstractGeneratedDataSource")
  }

}
