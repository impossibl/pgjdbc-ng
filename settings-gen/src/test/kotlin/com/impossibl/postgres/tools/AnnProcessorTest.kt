package com.impossibl.postgres.tools

import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler.javac
import com.google.testing.compile.JavaFileObjects
import kotlin.test.Ignore
import kotlin.test.Test


class AnnProcessorTest {

  @Test
  @Ignore
  fun testCompile() {

    val result = javac()
       .withProcessors(SettingsProcessor())
       .compile(JavaFileObjects.forResource("MySettingsTest.java"))

    assertThat(result).succeeded()
    assertThat(result)
       .generatedSourceFile("com.impossibl.postgres.jdbc.AbstractGeneratedDataSource")
  }

}
