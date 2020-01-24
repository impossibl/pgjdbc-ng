package com.impossibl.postgres.tools

import com.impossibl.postgres.api.jdbc.PGAnyType
import com.impossibl.postgres.api.jdbc.PGConnection
import com.impossibl.postgres.types.QualifiedName
import com.squareup.javapoet.*
import com.xenomachina.argparser.*
import java.io.File
import java.io.InputStream
import java.io.Reader
import java.sql.*
import java.util.*
import javax.lang.model.element.Modifier


class UDTGenerator(
   private val connection: PGConnection,
   private val targetPackage: String,
   typeNames: List<String>
) {

  companion object {

    private class Arguments(parser: ArgParser) {

      val types by parser.positionalList("TYPES", "type names")
      val url by parser.storing("--url", help = "Connection URL").default(null as String?)
      val host by parser.storing("-H", "--host", help = "Server host name").default(null as String?)
      val port by parser.storing("-T", "--port", help = "Server port number") { toInt() }.default(null as Int?)
      val db by parser.storing("-D", "--database", help = "Database name").default(null as String?)
      val user by parser.storing("-U", "--user", help = "Database username").default(null as String?)
      val password by parser.storing("-P", "--password", help = "Database password").default(null as String?)
      val outDirectory by parser.storing("-o", "--out", help = "Output directory") { File(this) }
         .default(File("out"))
         .addValidator {
           if (!value.exists())
             value.mkdirs()
           else if (!value.isDirectory)
             throw InvalidArgumentException("Out must be a directory")
         }
      val targetPackage by parser.storing("-p", "--pkg", help = "Target Java package")

    }

    @JvmStatic
    fun main(args: Array<String>) {

      try {

        ArgParser(args).parseInto(::Arguments).run {

          val url =
             if (url != null) {
               url!!
             } else {

               if (db == null) {
                 throw MissingValueException("missing DB or URL")
               }

               val url =
                  "jdbc:pgsql:${host?.let { host -> "//$host${port?.let { ":$it" } ?: ""}/" } ?: ""}${db ?: ""}"

               url
             }

          val props = Properties()
          user?.let { props.setProperty("user", it) }
          password?.let { props.setProperty("password", it) }

          UDTGenerator(url, props, targetPackage, types)
             .generate(outDirectory)
        }
      } catch (x: SystemExitException) {
        x.printAndExit("UDT Generator")
      }
    }

  }

  constructor(connectionUrl: String, connectionProperties: Properties? = null, targetPackage: String, types: List<String>)
     : this(
     DriverManager.getConnection(connectionUrl, connectionProperties ?: Properties()),
     targetPackage,
     types
  )

  constructor(connection: Connection, targetPackage: String, types: List<String>)
     : this(
     connection.unwrap(PGConnection::class.java)
        ?: throw IllegalArgumentException("Requires a compatible PGConnection"),
     targetPackage,
     types
  )

  private val typesInfo = getTypesInfo(connection, typeNames)

  fun generate(): List<JavaFile> {

    return typesInfo.mapNotNull { (sqlTypeName, typeCategory) ->

      when (typeCategory) {

        TypeCategory.Composite -> generatePOJO(sqlTypeName)

        TypeCategory.Enum -> generateEnum(sqlTypeName)

      }?.let { spec ->

        JavaFile.builder(targetPackage, spec)
           .skipJavaLangImports(true)
           .build()

      }

    }

  }

  fun generate(outDirectory: File) {
    generate().forEach { it.writeTo(outDirectory) }
  }

  private fun generateEnum(sqlTypeName: QualifiedName): TypeSpec? {

    val enums = getTypeEnumerations(connection, sqlTypeName)
    if (enums.isEmpty()) {
      System.out.println("Type `$sqlTypeName` contains no attributes")
      return null
    }

    val enumName = ClassName.get(targetPackage, sqlTypeName.localName.javaTypeName())

    val enumBldr = TypeSpec.enumBuilder(enumName)
       .addModifiers(Modifier.PUBLIC)
       .addField(
          FieldSpec.builder(String::class.java, "TYPE_NAME")
             .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
             .initializer("\$S", sqlTypeName.toString(false))
             .build()
       )
       .addField(
          FieldSpec.builder(PGAnyType::class.java, "TYPE", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
             .initializer("\$L",
                TypeSpec.anonymousClassBuilder("")
                   .addSuperinterface(ClassName.get(PGAnyType::class.java))
                   .addMethod(
                      MethodSpec.methodBuilder("getName")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(String::class.java))
                         .addCode("return \$T.TYPE_NAME;\n", enumName)
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getVendor")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(String::class.java))
                         .addCode("return \$S;\n", "UDT Generated")
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getVendorTypeNumber")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(Integer::class.java))
                         .addCode("return null;\n")
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getJavaType")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(Class::class.java))
                         .addCode("return \$T.class;\n", enumName)
                         .build()
                   )
                   .build()
             )
             .build()
       )
       .addField(
          FieldSpec.builder(String::class.java, "label")
             .addModifiers(Modifier.PRIVATE)
             .build()
       )
       .addMethod(
          MethodSpec.constructorBuilder()
             .addParameter(String::class.java, "label")
             .addStatement("this.label = label")
             .build()
       )
       .addMethod(
          MethodSpec.methodBuilder("getLabel")
             .addModifiers(Modifier.PUBLIC)
             .returns(String::class.java)
             .addStatement("return label")
             .build()
       )
       .addMethod(
          MethodSpec.methodBuilder("valueOfLabel")
             .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
             .returns(enumName)
             .addParameter(String::class.java, "label")
             .addCode("for (\$T value : values()) {\$>\n", enumName)
             .addCode("if (value.label.equals(label)) return value;\$<\n")
             .addCode("}\n")
             .addCode("throw new \$T(\$S);", java.lang.IllegalArgumentException::class.java, "Invalid label")
             .build()
       )

    enums.forEach {
      enumBldr.addEnumConstant(it.toUpperCase(), TypeSpec.anonymousClassBuilder("\$S", it).build())
    }

    return enumBldr.build()
  }

  private fun generatePOJO(sqlTypeName: QualifiedName): TypeSpec? {

    val attributes = getTypeAttributes(connection, sqlTypeName)
    if (attributes.isEmpty()) {
      System.out.println("Type `$sqlTypeName` contains no attributes")
      return null
    }

    val className = ClassName.get(targetPackage, sqlTypeName.localName.javaTypeName())

    val classBldr = TypeSpec.classBuilder(className)
       .addModifiers(Modifier.PUBLIC)
       .addSuperinterface(SQLData::class.java)
       .addField(
          FieldSpec.builder(String::class.java, "TYPE_NAME")
             .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
             .initializer("\$S", sqlTypeName.toString(false))
             .build()
       )
       .addField(
          FieldSpec.builder(PGAnyType::class.java, "TYPE", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
             .initializer("\$L",
                TypeSpec.anonymousClassBuilder("")
                   .addSuperinterface(ClassName.get(PGAnyType::class.java))
                   .addMethod(
                      MethodSpec.methodBuilder("getName")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(String::class.java))
                         .addCode("return \$T.TYPE_NAME;\n", className)
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getVendor")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(String::class.java))
                         .addCode("return \$S;\n", "UDT Generated")
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getVendorTypeNumber")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(Integer::class.java))
                         .addCode("return null;\n")
                         .build()
                   )
                   .addMethod(
                      MethodSpec.methodBuilder("getJavaType")
                         .addModifiers(Modifier.PUBLIC)
                         .addAnnotation(ClassName.get(Override::class.java))
                         .returns(ClassName.get(Class::class.java))
                         .addCode("return \$T.class;\n", className)
                         .build()
                   )
                   .build()
             )
             .build()
       )
       .addMethod(
          MethodSpec.methodBuilder("getSQLTypeName")
             .addAnnotation(Override::class.java)
             .addModifiers(Modifier.PUBLIC)
             .returns(String::class.java)
             .addException(SQLException::class.java)
             .addStatement("return TYPE_NAME")
             .build()
       )

    val readSQLBldr = MethodSpec.methodBuilder("readSQL")
       .addAnnotation(Override::class.java)
       .addException(SQLException::class.java)
       .addModifiers(Modifier.PUBLIC)
       .addParameter(SQLInput::class.java, "in")
       .addParameter(String::class.java, "typeName")
       .addException(SQLException::class.java)

    val writeSQLBldr = MethodSpec.methodBuilder("writeSQL")
       .addAnnotation(Override::class.java)
       .addException(SQLException::class.java)
       .addModifiers(Modifier.PUBLIC)
       .addParameter(SQLOutput::class.java, "out")
       .addException(SQLException::class.java)


    for (attr in attributes) {

      val attrPropName = attr.name.javaPropertyName()
      val attrSqlType = connection.resolveType(attr.typeName.toString())
      val attrTypeName = resolveTypeName(attr.typeName, attrSqlType)

      classBldr.addField(
         FieldSpec.builder(attrTypeName.box(), attrPropName)
            .addModifiers(Modifier.PRIVATE)
            .build()
      )

      classBldr.addMethod(
         MethodSpec.methodBuilder("get${attrPropName.capitalize()}")
            .addModifiers(Modifier.PUBLIC)
            .returns(attrTypeName)
            .addStatement("return \$L", attrPropName)
            .build()
      )

      classBldr.addMethod(
         MethodSpec.methodBuilder("set${attrPropName.capitalize()}")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(attrTypeName, attrPropName)
            .addStatement("this.\$1L = \$1L", attrPropName)
            .build()
      )

      readSQLBldr.addCode(
         when {
           attrTypeName is ArrayTypeName ->
             CodeBlock.of("this.\$L = in.readObject(\$T.class);\n", attrPropName, attrTypeName)

           attrSqlType.javaType.readerTypeName == "Object" ->
             CodeBlock.of("this.\$L = in.readObject(\$T.class);\n", attrPropName, attrTypeName)

           typesInfo[attr.typeName] == TypeCategory.Enum ->
             CodeBlock.of("this.\$L = \$T.valueOfLabel(in.readString());\n", attrPropName, attrTypeName)

           else ->
             if (attrTypeName.box().isBoxedPrimitive)
               CodeBlock.of("this.\$L = in.readObject(\$T.class);\n", attrPropName, attrSqlType.javaType)
             else
               CodeBlock.of("this.\$L = in.read\$L();\n", attrPropName, attrSqlType.javaType.readerTypeName)
         }
      )

      writeSQLBldr.addCode(
         when {
           attrTypeName is ArrayTypeName ->
             CodeBlock.of("out.writeObject(this.\$L, \$T.\$L);\n", attrPropName, JDBCType::class.java, "ARRAY")

           typesInfo[attr.typeName] == TypeCategory.Composite ->
             CodeBlock.of("out.writeObject(this.\$L, \$T.\$L);\n", attrPropName, attrTypeName, "TYPE")

           typesInfo[attr.typeName] == TypeCategory.Enum ->
             CodeBlock.of("out.writeObject(this.\$L.getLabel(), \$T.\$L);\n", attrPropName, attrTypeName, "TYPE")

           attrSqlType.javaType.writerTypeName == "Object" ->
             CodeBlock.of("out.writeObject(this.\$L, \$T.\$L);\n", attrPropName, JDBCType::class.java, "STRUCT")

           else ->
             if (attrTypeName.box().isBoxedPrimitive)
               CodeBlock.of("out.writeObject(this.\$L, \$T.\$L);\n", attrPropName, JDBCType::class.java, attrTypeName.primitiveJDBCType)
             else
               CodeBlock.of("out.write\$L(this.\$L);\n", attrSqlType.javaType.readerTypeName, attrPropName)
         }
      )

    }

    classBldr.addMethod(readSQLBldr.build())
    classBldr.addMethod(writeSQLBldr.build())

    return classBldr.build()
  }


  private fun resolveTypeName(srcTypeName: QualifiedName, sqlType: PGAnyType): TypeName =
     if (typesInfo.contains(srcTypeName)) {
       ClassName.get(targetPackage, srcTypeName.localName.javaTypeName())
     }
     else if (java.sql.Array::class.java.isAssignableFrom(sqlType.javaType)) {
       val elemSqlTypeName = getArrayElementType(connection, srcTypeName)
          ?: throw IllegalStateException("Cannot determine array element type: $srcTypeName")
       val elemSqlType = connection.resolveType(elemSqlTypeName.toString(false))
       ArrayTypeName.of(resolveTypeName(elemSqlTypeName, elemSqlType))
     }
     else {
       if (sqlType.javaType.isPrimitive)
         TypeName.get(sqlType.javaType).box()
       else
         ClassName.get(sqlType.javaType)
     }

}


private val <T> Class<T>.writerTypeName: String get() = this.readerTypeName

private val <T> Class<T>.readerTypeName: String
  get() =
    when (this) {
      Array<Byte>::class.java -> "Bytes"
      Integer::class.java -> "Int"
      Reader::class.java -> "CharacterStream"
      InputStream::class.java -> "BinaryStream"
      Struct::class.java -> "Object"

      else ->
        if (`package`.name == "java.lang" || `package`.name == "java.sql") {
          simpleName
        } else {
          "Object"
        }
    }


enum class TypeCategory {
  Composite,
  Enum,
}


private fun getTypesInfo(connection: Connection, typeNames: List<String>): Map<QualifiedName, TypeCategory> {
  connection.prepareStatement(
     """
        select
          nspname, typname, typcategory
        from pg_type t
         left join pg_namespace n on (n.oid = typnamespace)
        where t.oid = ?::text::regtype;
    """.trimMargin()
  ).use { stmt ->
    return typeNames.mapNotNull { typeName ->
      stmt.setString(1, typeName)
      stmt.executeQuery().use { rs ->
        if (rs.next()) {
          val schemaName = rs.getString(1)
          val localName = rs.getString(2)
          val category = when (rs.getString(3)) {
            "C" -> TypeCategory.Composite
            "E" -> TypeCategory.Enum
            else -> throw InvalidArgumentException("Type category not supported for $typeName")
          }
          QualifiedName(schemaName, localName) to category
        }
        else
          null
      }
    }.toMap()
  }
}

private fun getArrayElementType(connection: Connection, typeName: QualifiedName): QualifiedName? {
  connection.prepareStatement(
     """
       SELECT
        n.nspname, e.typname
       FROM pg_type a
        LEFT JOIN pg_type e ON (a.typelem = e.oid)
        LEFT JOIN pg_namespace n ON (e.typnamespace = n.oid)
       WHERE a.oid = ?::text::regtype
     """.trimIndent()
  ).use { stmt ->
    stmt.setString(1, typeName.toString(false))
    stmt.executeQuery().use { rs ->
      if (!rs.next()) return null
      return QualifiedName(rs.getString(1), rs.getString(2))
    }
  }
}

private fun getTypeEnumerations(connection: Connection, typeName: QualifiedName): List<String> {

  connection.prepareStatement(
     "SELECT enumlabel FROM pg_enum WHERE enumtypid = ?::text::regtype ORDER BY enumsortorder"
  ).use { stmt ->
    stmt.setString(1, typeName.toString(false))
    stmt.executeQuery().use { rs ->
      val enums = mutableListOf<String>()
      while (rs.next()) {
        enums.add(rs.getString(1))
      }
      return enums
    }
  }
}

private data class TypeAttribute(
   val name: String,
   val typeName: QualifiedName,
   val number: Int,
   val nullable: Boolean,
   val isStruct: Boolean
)

private fun getTypeAttributes(connection: Connection, typeName: QualifiedName): List<TypeAttribute> {

  val sql =
     """
       SELECT
          attname as name, n.nspname as type_namespace, typname as type_name, attnum as number,
          not attnotnull as nullable, case when typtype = 'c' then true else false end as is_struct
       from pg_catalog.pg_attribute a
        LEFT JOIN pg_catalog.pg_type t ON (atttypid = t.oid)
        LEFT JOIN pg_namespace n ON (t.typnamespace = n.oid)
       where
        attrelid = ?::text::regclass and attnum > 0 and not attisdropped
       order by attnum
     """.trimIndent()

  val attrs = mutableListOf<TypeAttribute>()

  connection.prepareStatement(sql).use { stmt ->
    stmt.setString(1, typeName.toString())
    stmt.executeQuery().use { rs ->
      while (rs.next()) {

        val attr = TypeAttribute(
           rs.getString("name"),
           QualifiedName(rs.getString("type_namespace"), rs.getString("type_name")),
           rs.getInt("number"),
           rs.getBoolean("nullable"),
           rs.getBoolean("is_struct")
        )

        attrs.add(attr)
      }
    }
  }

  return attrs
}

private fun String.javaTypeName(): String {
  return this.split("""[-_.]""".toRegex()).joinToString("") { it.toLowerCase().capitalize() }
}

private fun String.javaPropertyName(): String {
  return javaTypeName().decapitalize()
}

private val TypeName.primitiveJDBCType: JDBCType
  get() =
    when (if (isBoxedPrimitive) unbox() else this) {
      TypeName.BOOLEAN -> JDBCType.BOOLEAN
      TypeName.BYTE -> JDBCType.TINYINT
      TypeName.SHORT -> JDBCType.SMALLINT
      TypeName.INT -> JDBCType.INTEGER
      TypeName.LONG -> JDBCType.BIGINT
      TypeName.FLOAT -> JDBCType.REAL
      TypeName.DOUBLE -> JDBCType.DOUBLE
      else -> throw IllegalArgumentException("Not a primitive/box type")
    }
