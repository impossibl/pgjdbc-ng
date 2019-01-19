
import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler.javac
import com.google.testing.compile.JavaFileObjects
import com.impossibl.postgres.tools.UDTGenerator
import org.junit.Test
import java.io.File
import java.sql.DriverManager
import java.util.*
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UDTGeneratorTest {

  @Test
  fun testCompile() {

    val props = Properties()
    props.setProperty("user", "test")
    props.setProperty("password", "test")

    DriverManager.getConnection("jdbc:pgsql:test", props).use { connection ->

      val schemaName = "test${Random.nextInt(0, 0xffffff).toString(16)}"
      try {

        connection.createStatement().use { stmt ->
          stmt.execute("CREATE SCHEMA $schemaName")
          stmt.execute("CREATE TYPE $schemaName.title as enum ('mr', 'mrs', 'ms', 'dr')")
          stmt.execute("CREATE TYPE $schemaName.address as (street text, city text, state char(2), zip char(5))")
          stmt.execute("CREATE TYPE $schemaName.v_card as (id int4, name text, title $schemaName.title, addresses $schemaName.address[])")
          stmt.execute("SET SEARCH_PATH = public, $schemaName")
        }

        val pkgName = "udt.test"

        val files = UDTGenerator(connection, pkgName, listOf("$schemaName.v_card", "address", "title"))
           .generate()
           .map { it.toJavaFileObject() }

        val result = javac()
           .compile(files + JavaFileObjects.forResource("VCardTest.java"))

        assertThat(result).succeeded()

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

    val props = Properties()
    props.setProperty("user", "test")
    props.setProperty("password", "test")

    DriverManager.getConnection("jdbc:pgsql:test", props).use { connection ->

      val schemaName = "test${Random.nextInt(0, 0xffffff).toString(16)}"
      try {

        connection.createStatement().use { stmt ->
          stmt.execute("CREATE SCHEMA $schemaName")
          stmt.execute("CREATE TYPE $schemaName.\"TITLE\" as enum ('mr', 'mrs', 'ms', 'dr')")
          stmt.execute("CREATE TYPE $schemaName.address as (street text, city text, state char(2), zip char(5))")
          stmt.execute("CREATE TYPE $schemaName.v_card as (id int4, name text, title $schemaName.\"TITLE\", addresses $schemaName.address[])")
          stmt.execute("SET SEARCH_PATH = public, $schemaName")
        }

        val outDirectory = File("target/test/generated")
        outDirectory.deleteRecursively()

        val pkgName = "udt.test"

        UDTGenerator(connection, pkgName, listOf("$schemaName.v_card", "address", "\"$schemaName\".\"TITLE\""))
           .generate(outDirectory)

        val pkgFileNames = File(outDirectory, pkgName.replace('.', '/'))
           .listFiles()
           .map { it.name }

        assertEquals(3, pkgFileNames.size)
        assertTrue(pkgFileNames.contains("Title.java"))
        assertTrue(pkgFileNames.contains("Address.java"))
        assertTrue(pkgFileNames.contains("VCard.java"))

      }
      finally {
        connection.createStatement().use {
          it.execute("DROP SCHEMA $schemaName CASCADE")
        }
      }
    }

  }

}
