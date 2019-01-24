package com.impossibl.jdbc.spy.tools

import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler.javac
import com.google.testing.compile.JavaFileObjects
import org.junit.jupiter.api.Test
import javax.tools.JavaFileObject


class SpyGenTest {

  @Test
  fun testGen() {

    val supportFiles = listOf<JavaFileObject>(
       JavaFileObjects.forResource("Trace.java"),
       JavaFileObjects.forResource("TraceOutput.java"),
       JavaFileObjects.forResource("Relay.java")
    )

    val generatedFiles = SpyGen().generate().map { it.toJavaFileObject() }

    val result = javac()
       .compile(generatedFiles + supportFiles)

    assertThat(result).succeeded()
  }

}
