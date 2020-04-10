package com.impossibl.postgres.tools.test

import com.google.testing.compile.Compilation
import com.google.testing.compile.Compiler.javac
import com.google.testing.compile.JavaFileObjects
import com.impossibl.postgres.tools.UDTGenerator
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.hasItems
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.sql.DriverManager
import java.util.*
import javax.tools.Diagnostic
import kotlin.random.Random

class UDTGeneratorTest {

  companion object {

    private val server = System.getProperty("pgjbdc.test.server", "localhost")
    private val port = System.getProperty("pgjdbc.test.port", "5432")
    private val db = System.getProperty("pgjdbc.test.db", "test")
    private val url = "jdbc:pgsql://$server:$port/$db"

    private val props = Properties().apply {
      setProperty("user", System.getProperty("pgjdbc.test.user", "test"))
      setProperty("password", System.getProperty("pgjdbc.test.password", "test"))
    }

  }

  @Test
  fun testCompile() {
    println("Generating code from $url")

    DriverManager.getConnection(url, props).use { connection ->

      val schemaName = "test${Random.nextInt(0, 0xffffff).toString(16)}"
      try {

        connection.createStatement().use { stmt ->
          stmt.execute("CREATE SCHEMA $schemaName")
          stmt.execute("CREATE TYPE $schemaName.title as enum ('mr', 'mrs', 'ms', 'dr')")
          stmt.execute("CREATE TYPE $schemaName.address as (street varchar, city text, state char(2), zip char(5))")
          stmt.execute("CREATE TYPE $schemaName.v_card as (id int4, name text, title $schemaName.title, addresses $schemaName.address[])")
          stmt.execute("SET SEARCH_PATH = public, $schemaName")
        }

        val pkgName = "udt.test"

        val files = UDTGenerator(connection, pkgName, listOf("$schemaName.v_card", "address", "title"))
           .generate()
           .map { it.toJavaFileObject() }

        val result = javac()
           .compile(files + JavaFileObjects.forResource("VCardTest.java"))

        assertThat(result.errors(), equalTo(emptyList<Diagnostic<*>>()))
        assertThat(result.status(), equalTo(Compilation.Status.SUCCESS))

      }
      finally {
        connection.createStatement().use {
          it.execute("DROP SCHEMA $schemaName CASCADE")
        }
      }
    }

  }

  @Test
  fun testGenerate() {
    println("Generating code from $url")

    DriverManager.getConnection(url, props).use { connection ->

      val schemaName = "test${Random.nextInt(0, 0xffffff).toString(16)}"
      try {

        connection.createStatement().use { stmt ->
          stmt.execute("CREATE SCHEMA $schemaName")
          stmt.execute("CREATE TYPE $schemaName.\"TITLE\" as enum ('mr', 'mrs', 'ms', 'dr')")
          stmt.execute("CREATE TYPE $schemaName.address as (street text, city text, state char(2), zip char(5))")
          stmt.execute("CREATE TYPE $schemaName.v_card as (id int4, name text, title $schemaName.\"TITLE\", addresses $schemaName.address[])")
          stmt.execute("SET SEARCH_PATH = public, $schemaName")
        }

        val outDirectory = File("build/test/generated")
        outDirectory.deleteRecursively()

        val pkgName = "udt.test"

        UDTGenerator(connection, pkgName, listOf("$schemaName.v_card", "address", "\"$schemaName\".\"TITLE\""))
           .generate(outDirectory)

        val pkgFileNames = File(outDirectory, pkgName.replace('.', '/'))
           .listFiles()!!
           .map { it.name }

        assertThat(pkgFileNames.size, equalTo(3))
        assertThat(pkgFileNames, hasItems("Title.java", "Address.java", "VCard.java"))

      }
      finally {
        connection.createStatement().use {
          it.execute("DROP SCHEMA $schemaName CASCADE")
        }
      }
    }

  }

}
