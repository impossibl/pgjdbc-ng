package com.impossibl.postgres.tools

import com.impossibl.postgres.tools.SettingsProcessor.Companion.SETTING_TYPE_NAME
import com.squareup.javapoet.*
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.StringWriter
import java.nio.charset.Charset
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.function.Function
import javax.annotation.processing.*
import javax.lang.model.SourceVersion.RELEASE_8
import javax.lang.model.element.*
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.TypeMirror
import javax.lang.model.util.Elements
import javax.lang.model.util.Types
import javax.tools.Diagnostic
import javax.tools.StandardLocation


@SupportedAnnotationTypes(
   "$SETTING_TYPE_NAME.Factory"
)
@SupportedSourceVersion(RELEASE_8)
class SettingsProcessor : AbstractProcessor() {

  data class FactoryInfo(val groups: Set<GroupInfo>, val settings: Set<SettingInfo>, val element: Element)

  data class GroupInfo(val id: String, val desc: String, val global: Boolean, val order: Int,
                       val field: VariableElement)

  data class SettingInfo(val name: String, val group: String, val type: TypeMirror, val desc: String,
                         val default: String?, val dynamicDefaultCode: String?, val staticDefaultCode: String?,
                         val min: Int?, val max: Int?, val alternateNames: List<String>?, val field: VariableElement)

  companion object {

    private const val PG_PKG = "com.impossibl.postgres"
    private const val SYSTEM_PKG = "$PG_PKG.system"
    private const val JDBC_PKG = "$PG_PKG.jdbc"
    const val SETTING_TYPE_NAME = "$SYSTEM_PKG.Setting"
    const val SETTING_TYPE_ANN_NAME = "$SETTING_TYPE_NAME.Info"
    const val SETTING_GROUP_TYPE_NAME = "$SETTING_TYPE_NAME.Group"
    const val SETTING_GROUP_TYPE_ANN_NAME = "$SETTING_GROUP_TYPE_NAME.Info"

    const val JDBC_DS_GROUP_NAME = "jdbc-ds"
    const val SYSTEM_PROPERTY_PREFIX = "pgjdbc."

  }

  private lateinit var types: Types
  private lateinit var elements: Elements
  private lateinit var filer: Filer
  private lateinit var messager: Messager

  private var generatedAnn: TypeElement? = null
  private lateinit var factoryAnn: TypeElement
  private lateinit var descriptionAnn: TypeElement
  private lateinit var descriptionAnnType: TypeMirror
  private lateinit var settingType: TypeMirror
  private lateinit var settingGroupType: TypeMirror
  private lateinit var settingAnn: TypeElement
  private lateinit var settingAnnType: TypeMirror
  private lateinit var settingGroupAnn: TypeElement
  private lateinit var settingGroupAnnType: TypeMirror

  private val converterTypeName = ClassName.get(SYSTEM_PKG, "Setting", "Converter")
  private val funcTypeName = ClassName.get(Function::class.java)
  private val versionTypeName = ClassName.get(SYSTEM_PKG, "Version")
  private val strTxmsTypeName = ClassName.get("$PG_PKG.utils", "StringTransforms")
  private val dsTypeName = ClassName.get(JDBC_PKG, "AbstractDataSource")

  override fun init(processingEnv: ProcessingEnvironment) {
    synchronized(this) {
      super.init(processingEnv)

      types = processingEnv.typeUtils
      elements = processingEnv.elementUtils
      filer = processingEnv.filer
      messager = processingEnv.messager
    }
  }

  override fun process(annotations: MutableSet<out TypeElement>, roundEnv: RoundEnvironment): Boolean {

    cacheElementsAndTypes()

    val factories = gatherFactories(roundEnv)
    if (factories.isEmpty()) return true

    val allFactories = updateKnownFactories(factories)

    if (!validate(allFactories)) return false

    if (!generateFactoryInitializers(allFactories)) return false

    val globalGroups = allFactories.flatMap { it.groups }.filter { it.global }.toSet()
    val allSettings = allFactories.flatMap { it.settings }.toSet()

    if (!generateDataSourceWithProperties(globalGroups, allSettings)) return false

    if (!generateSettingsDoc(globalGroups, allSettings)) return false

    return true
  }

  private fun updateKnownFactories(factories: Set<FactoryInfo>): Set<FactoryInfo> {

    val knownFactoryNames =
       factories
          .map { (it.element as TypeElement).qualifiedName.toString() }
          .toMutableSet()

    val knownFactories = factories.toMutableSet()

    try {
      val previouslyKnownFactoryNames =
         filer.getResource(StandardLocation.SOURCE_OUTPUT, "", "SettingsProcessor.known")
            .getCharContent(true)
            .split("\n")

      messager.printMessage(Diagnostic.Kind.NOTE, "Loaded previously known settings factories")

       val previouslyKnownFactories =
          previouslyKnownFactoryNames
             .mapNotNull { elements.getTypeElement(it) }
             .map { gatherSettingsAndGroups(it) }
             .toSet()

      knownFactoryNames += previouslyKnownFactories.map { (it.element as TypeElement).qualifiedName.toString() }
      knownFactories += previouslyKnownFactories
    }
    catch (x: IOException) {
      // No previous file... use current set
    }

    // Save new complete list

    val file = filer.createResource(StandardLocation.SOURCE_OUTPUT, "", "SettingsProcessor.known")
    file.openWriter()
       .use { out ->
         knownFactoryNames.forEach { out.append(it).append("\n") }
       }

    return knownFactories
  }

  private fun cacheElementsAndTypes() {
    generatedAnn =
       elements.getTypeElement("javax.annotation.processing.Generated") ?:
       elements.getTypeElement("javax.annotation.Generated")
    factoryAnn = elements.getTypeElement("$SETTING_TYPE_NAME.Factory")
    descriptionAnn = elements.getTypeElement("$SETTING_TYPE_NAME.Description")
    descriptionAnnType = descriptionAnn.asType()
    settingType = elements.getTypeElement(SETTING_TYPE_NAME).asType()
    settingAnn = elements.getTypeElement(SETTING_TYPE_ANN_NAME)
    settingAnnType = settingAnn.asType()
    settingGroupType = elements.getTypeElement(SETTING_GROUP_TYPE_NAME).asType()
    settingGroupAnn = elements.getTypeElement(SETTING_GROUP_TYPE_ANN_NAME)
    settingGroupAnnType = settingGroupAnn.asType()
  }

  private fun generateSettingsDoc(groups: Set<GroupInfo>, settings: Set<SettingInfo>): Boolean {

    OutputStreamWriter(FileOutputStream("./SETTINGS.md"))
       .use { out ->

         val orderedGroups = groups.sortedBy { it.order }

         out.append(
            "NOTE: This file is autogenerated from code based on " +
               "${orderedGroups.map { it.field.enclosingElement }.toSet().joinToString { "`${it.simpleName}.java`" }}. " +
               "If there are any errors or omissions they need to be addressed in code; not by editing this file.\n\n"
         )

         out.append(
            "Table of Contents\n\n"
         )

         for (group in orderedGroups) {
           out.append("* [").append(group.desc).append("](#").append(group.desc.markdownAnchor()).append(")\n")
         }

         out.append("\n")

         for (group in orderedGroups) {

           out.append("## ").append(group.desc)
              .append("\n\n")

           for (setting in settings) {
             if (setting.group != group.id) continue

             out.append("#### ").append(setting.name.toTitleCase())
                .append("\n\n")

             out.append(setting.desc.escapeMarkdown())
                .append("\n\n")

             if (setting.type.isEnum) {
               val enumElement = (setting.type as? DeclaredType)!!.asElement() as TypeElement
               out.append(enumDocumentation(enumElement))
                  .append("\n\n")
             }

             if (group.id != JDBC_DS_GROUP_NAME) {
               out.append("Driver Property: `").append(setting.name).append("`")
                  .append("\n\n")
             }

             val typeName = ClassName.get(setting.type)

             out.append("DataSource: ")
                .append("`get").append(setting.name.beanPropertyName.capitalize()).append("()`/")
                .append("`set").append(setting.name.beanPropertyName.capitalize()).append("(").append(typeName.toString()).append(")`")
                .append("\n\n")

             out.append("System Property: `").append(SYSTEM_PROPERTY_PREFIX).append(
                setting.name).append("`")
                .append("\n\n")

             if (setting.min != null && setting.max != null) {
               out.append("Range: Must be between `")
                  .append(setting.min.toString())
                  .append("` and `")
                  .append(setting.max.toString())
                  .append("`\n\n")
             }
             else if (setting.min != null) {
               out.append("Range: Must be greater than or equal to `")
                  .append(setting.min.toString())
                  .append("`\n\n")
             }
             else if (setting.max != null) {
               out.append("Range: Must be less than or equal to `")
                  .append(setting.min.toString())
                  .append("`\n\n")
             }

             if (setting.dynamicDefaultCode == null && setting.staticDefaultCode == null) {
               val default =
                  if (setting.default != null)
                    if (!setting.default.isEmpty())
                      "`${setting.default}`"
                    else
                      "Empty"
                  else
                    "None"
               out.append("Default: ").append(default)
                  .append("\n\n")
             }
             else if (setting.default != null) {
               out.append("Default: ").append(setting.default)
                  .append("\n\n")
             }
           }

         }

       }

    return true
  }

  private fun generateFactoryInitializers(factories: Set<FactoryInfo>): Boolean {

    for (factory in factories) {

      if (!generateFactoryInitializer(factory)) return false

    }

    val globalGroups = factories.flatMap { it.groups }.filter { it.global }.toSet()

    val globalClass = TypeSpec.interfaceBuilder("SettingGlobal")
       .apply {
         if (generatedAnn != null) {
           addAnnotation(
              AnnotationSpec.builder(ClassName.get(generatedAnn))
                 .addMember("value", "{\$S, \$S}",
                    SettingsProcessor::class.java.canonicalName,
                    "PGJDBC-NG Settings Annotation Processor")
                 .addMember("date", "\$S", DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.now()))
                 .addMember("comments", "\$S", "Generated from all global Setting.Group(s)" )
                 .build()
           )
         }
       }
       .addMethod(
          MethodSpec.methodBuilder("getAllGroups")
             .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
             .returns(ArrayTypeName.of(ClassName.get(settingGroupType)))
             .addCode("\$[return new Setting.Group[] {\n")
             .apply {
               globalGroups.forEachIndexed { index, group ->
                 addCode("\$T.\$L", ClassName.get(group.field.enclosingElement.asType()), group.field.simpleName.toString())
                 if (index < globalGroups.size - 1) {
                   addCode(",\n")
                 }
               }
             }
             .addCode("\$]\n};\n")
             .build()
       )
       .build()

    JavaFile.builder(SYSTEM_PKG, globalClass)
       .skipJavaLangImports(true)
       .build()
       .writeTo(filer)

    return true
  }

  private fun generateFactoryInitializer(factory: FactoryInfo): Boolean {

    val initClassBldr = TypeSpec.interfaceBuilder("${factory.element.simpleName}Init")
       .apply {
         if (generatedAnn != null) {
           addAnnotation(
              AnnotationSpec.builder(ClassName.get(generatedAnn))
                 .addMember("value", "{\$S, \$S}",
                            SettingsProcessor::class.java.canonicalName,
                            "PGJDBC-NG Settings Annotation Processor")
                 .addMember("date", "\$S", DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.now()))
                 .addMember("comments", "\$S", "Generated from Settings & Groups defined in ${factory.element.simpleName}" )
                 .build()
           )
         }
       }

    val initCode = CodeBlock.builder()

    for (group in factory.groups) {

      val ownerTypeName = ClassName.get(group.field.enclosingElement.asType())
      val groupFieldName = group.field.simpleName.toString()

      initCode.addStatement("\$T.\$L.init(\$S, \$S, \$L)", ownerTypeName, groupFieldName, group.id, group.desc,
                              group.global)
    }

    var checkRange: Boolean = false

    for (setting in factory.settings) {

      val ownerTypeName = ClassName.get(setting.field.enclosingElement.asType())
      val settingFieldName = setting.field.simpleName.toString()
      val settingTypeName = TypeName.get(setting.type)

      val fromString = CodeBlock.builder()
      val toString = CodeBlock.builder()

      when (settingTypeName) {

        ClassName.get(java.lang.String::class.java) -> {
          fromString.add("\$T.identity()", converterTypeName)
          toString.add("\$T.identity()", funcTypeName)
        }

        ClassName.get(java.lang.Integer::class.java) -> {
          if (setting.min != null || setting.max != null) {
            fromString.add("str -> checkRange(\$S, \$T.parseInt(str), \$L, \$L)", setting.name, settingTypeName, setting.min, setting.max)
            checkRange = true
          }
          else {
            fromString.add("\$T::parseInt", settingTypeName)
          }
          toString.add("\$T::toString", TypeName.OBJECT)
        }

        ClassName.get(java.lang.Boolean::class.java) -> {
          fromString.add("\$T::parseBoolean", settingTypeName)
          toString.add("\$T::toString", TypeName.OBJECT)
        }

        ClassName.get(Charset::class.java) -> {
          fromString.add("\$T::forName", settingTypeName)
          toString.add("\$T::name", settingTypeName)
        }

        ClassName.get(java.lang.Class::class.java) -> {
          fromString.add("\$T::forName", settingTypeName)
          toString.add("\$T::getName", settingTypeName)
        }

        versionTypeName -> {
          fromString.add("\$T::parse", versionTypeName)
          toString.add("\$T::toString", versionTypeName)
        }

        else ->

          if (setting.type.isEnum) {

            if (setting.type.isEnumUpperSnakeCase) {

              fromString.add("str -> \$T.valueOf(\$T.class, \$T.toUpperSnakeCase(str))",
                             ClassName.get(java.lang.Enum::class.java), settingTypeName, strTxmsTypeName)
              toString.add("e -> \$T.dashedFromSnakeCase(e.name())", strTxmsTypeName)
            }
            else {

              fromString.add("str -> \$T.valueOf(\$T.class, \$T.toUpperCamelCase(str))",
                             ClassName.get(java.lang.Enum::class.java), settingTypeName, strTxmsTypeName)
              toString.add("e -> \$T.dashedFromCamelCase(e.name())", strTxmsTypeName)
            }
          }
          else {
            messager.printMessage(Diagnostic.Kind.ERROR, "Unsupported Setting Type", setting.field)
            return false
          }
      }

      val names = listOf(setting.name) + (setting.alternateNames ?: emptyList())

      val default =
         if (setting.dynamicDefaultCode != null) {
           CodeBlock.of("() -> ${setting.dynamicDefaultCode.escapePoet()}")
         }
         else if (setting.staticDefaultCode != null) {
           CodeBlock.of(setting.staticDefaultCode.escapePoet())
         }
         else if (setting.default != null) {
           CodeBlock.of("\$S", setting.default)
         }
         else {
           CodeBlock.of("(\$T) null", ClassName.get(String::class.java))
         }

      initCode.addStatement("\$T.\$L.init(\$S, \$S, \$T.class, \$L, \$L, \$L, new String[] {\$L})",
                              ownerTypeName, settingFieldName, setting.group, setting.desc, settingTypeName,
                              default, fromString.build(), toString.build(), names.joinToString { """"$it"""" })
    }

    initClassBldr.addMethod(
       MethodSpec.methodBuilder("init")
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .addCode(initCode.build())
          .build()
    )

    if (checkRange) {
      initClassBldr.addMethod(
         MethodSpec.methodBuilder("checkRange")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(ClassName.get(String::class.java), "name")
            .addParameter(TypeName.INT.box(), "value")
            .addParameter(TypeName.INT.box(), "min")
            .addParameter(TypeName.INT.box(), "max")
            .returns(TypeName.INT.box())
            .addStatement("if (min != null && value < min) throw new IllegalArgumentException(\"Setting '\" + name  + \"' is below minimum value of \" + min)")
            .addStatement("if (max != null && value > max) throw new IllegalArgumentException(\"Setting '\" + name  + \"' is above maximum value of \" + max)")
            .addStatement("return value")
            .build()
      )
    }

    JavaFile.builder(elements.getPackageOf(factory.element).qualifiedName.toString(), initClassBldr.build())
       .skipJavaLangImports(true)
       .build()
       .writeTo(filer)

    return true
  }

  private fun generateDataSourceWithProperties(groups: Set<GroupInfo>, settings: Set<SettingInfo>): Boolean {

    val groupIds = groups.map { it.id }.toSet()

    val dsClassBldr = TypeSpec.classBuilder("AbstractGeneratedDataSource")
       .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
       .superclass(dsTypeName)
       .apply {
         if (generatedAnn != null) {
           addAnnotation(
              AnnotationSpec.builder(ClassName.get(generatedAnn))
                 .addMember("value", "{\$S, \$S}",
                            SettingsProcessor::class.java.canonicalName,
                            "PGJDBC-NG Settings Annotation Processor")
                 .addMember("date", "\$S", DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.now()))
                 .addMember("comments", "\$S", "Generated from Setting(s) defined in the global Setting.Group(s)")
                 .build()
           )
         }
       }
       .addAnnotation(
          AnnotationSpec.builder(SuppressWarnings::class.java)
             .addMember("value", "{\$S, \$S}", "unused", "WeakerAccess")
             .build()
       )

    val added = mutableSetOf<SettingInfo>()

    for (setting in settings) {
      if (!groupIds.contains(setting.group)) continue

      if (added.contains(setting)) continue
      added.add(setting)

      val typeName = ClassName.get(setting.type)
      val owner = ClassName.get(setting.field.enclosingElement.asType())

      val paramType =
         if (typeName.isPrimitive || typeName.isBoxedPrimitive)
           setting.type
         else
           elements.getTypeElement(String::class.java.canonicalName).asType()

      val paramTypeName = ClassName.get(paramType)

      val getterTypeName =
         if (setting.dynamicDefaultCode == null && setting.default != null && paramTypeName.isBoxedPrimitive)
           paramTypeName.unbox()
         else
           paramTypeName

      val settingsGetterName = if (paramType != setting.type) "getText" else "get"

      val getter = MethodSpec.methodBuilder("get" + setting.name.beanPropertyName.capitalize())
         .addJavadoc(setting.desc.escapePoet() + "\n\n")
         .addJavadoc("@see #set\$L\n", setting.name.beanPropertyName.capitalize())
         .addJavadoc("@return Current value of \$L\n", setting.name.beanPropertyName)
         .addModifiers(Modifier.PUBLIC)
         .returns(getterTypeName)
         .addCode(
            CodeBlock.builder()
               .addStatement("return settings.\$L(\$T.\$L)", settingsGetterName, owner,
                             setting.field.simpleName.toString())
               .build()
         )

      dsClassBldr.addMethod(getter.build())

      val setterTypeName =
         if (paramTypeName.isBoxedPrimitive)
           paramTypeName.box()
         else
           paramTypeName

      val settingsSetterName = if (paramType != setting.type) "setText" else "set"

      val setter = MethodSpec.methodBuilder("set" + setting.name.beanPropertyName.capitalize())
         .addJavadoc(setting.desc.escapePoet() + "\n\n")
         .apply {
           if (setting.type.isEnum) {
             val enumElement = (setting.type as? DeclaredType)!!.asElement() as TypeElement
             addJavadoc(enumDocumentation(enumElement) + "\n\n")
           }
         }
         .addJavadoc("@param \$L New value of \$L\n", setting.name.beanPropertyName, setting.name.beanPropertyName)
         .addModifiers(Modifier.PUBLIC)
         .addParameter(setterTypeName, setting.name.beanPropertyName)
         .addCode(
            CodeBlock.builder()
               .addStatement("settings.\$L(\$T.\$L, \$L)", settingsSetterName, owner,
                             setting.field.simpleName.toString(), setting.name.beanPropertyName)
               .build()
         )

      dsClassBldr.addMethod(setter.build())
    }

    val dsClass = dsClassBldr.build()

    JavaFile.builder("com.impossibl.postgres.jdbc", dsClass)
       .skipJavaLangImports(true)
       .build()
       .writeTo(filer)

    return true
  }

  private fun enumDocumentation(element: TypeElement): String {

    val valueFormatter: (String) -> String =
       if (element.asType().isEnumUpperSnakeCase)
        String::dashesFromSnakeCase
       else
        String::dashesFromCamelCase

    return StringWriter()
       .use { out ->

         out.append("<ul>\n")

         for (enumConst in element.enclosedElements.filter { it.kind == ElementKind.ENUM_CONSTANT }) {

           val value = enumConst.simpleName.toString()

           out.append(" <li>\n")
              .append("  <code>").append(valueFormatter(value)).append("</code>")

           val description = enumConst.annotationMirrors.firstOrNull { it.annotationType == descriptionAnnType }
           if (description != null) {
             val descriptionValue = description.elementValues.values.first().value as String
             out.append(" - \n   ").append(descriptionValue.trim().replace("\n", "\n   ")).append("\n")
           }

           out.append(" </li>")
              .append("\n")


         }

         out.append("</ul>\n")
       }
       .toString()
  }

  private fun validate(factories: Set<FactoryInfo>): Boolean {

    val allGroups = factories.flatMap { it.groups }.toSet()

    var valid = true
    val globalSettingNames = mutableSetOf<String>()

    for (factory in factories) {

      for (setting in factory.settings) {

        // Check that setting references a defined group

        val group = allGroups.find { it.id == setting.group }
        if (group == null) {
          messager.printMessage(Diagnostic.Kind.ERROR, "Setting '${setting.name}' references undefined group '${setting.group}'", setting.field)
          valid = false
          continue
        }

        // If global, check name is unique

        if (group.global && globalSettingNames.contains(setting.name)) {
          messager.printMessage(Diagnostic.Kind.ERROR, "Setting name '${setting.name}' is duplicated", setting.field)
          valid = false
          continue
        }
        globalSettingNames.add(setting.name)

      }

    }

    return valid
  }

  private fun gatherFactories(roundEnv: RoundEnvironment): Set<FactoryInfo> {

    val factories = mutableSetOf<FactoryInfo>()

    for (factoryElement in roundEnv.getElementsAnnotatedWith(factoryAnn)) {
      val factory = gatherSettingsAndGroups(factoryElement)
      factories.add(factory)
    }

    return factories
  }

  private fun gatherSettingsAndGroups(factoryElement: Element): FactoryInfo {

    val groups = linkedSetOf<GroupInfo>()
    val settings = linkedSetOf<SettingInfo>()

    for (fieldElement in factoryElement.enclosedElements.filter { it.kind == ElementKind.FIELD }) {
      if (fieldElement !is VariableElement) continue

      if (fieldElement.isSetting) {

        val settingFieldType = fieldElement.asType() as? DeclaredType ?: continue

        val settingInfo =
                fieldElement.annotationMirrors.firstOrNull { types.isSameType(it.annotationType, settingAnnType) } ?: continue

        val settingType = settingFieldType.typeArguments.first()

        val elementValues = settingInfo.elementValues

        val settingName = elementValues[settingAnn.getAnnotationMethod("name")]!!.value as String
        val settingGroup = elementValues[settingAnn.getAnnotationMethod("group")]!!.value as String
        val settingDesc = elementValues[settingAnn.getAnnotationMethod("desc")]!!.value as String
        val settingDef = elementValues[settingAnn.getAnnotationMethod("def")]?.value as? String
        val settingDefDyn = elementValues[settingAnn.getAnnotationMethod("defDynamic")]?.value as? String
        val settingDefStatic = elementValues[settingAnn.getAnnotationMethod("defStatic")]?.value as? String
        val settingMin = elementValues[settingAnn.getAnnotationMethod("min")]?.value as? Int
        val settingMax = elementValues[settingAnn.getAnnotationMethod("max")]?.value as? Int
        @Suppress("UNCHECKED_CAST") val settingAlts =
                (elementValues[settingAnn.getAnnotationMethod("alternateNames")]?.value as? List<AnnotationValue>)?.map { it.value } as? List<String>

        settings.add(SettingInfo(settingName, settingGroup, settingType, settingDesc,
                settingDef, settingDefDyn, settingDefStatic, settingMin, settingMax, settingAlts, fieldElement))
      } else if (fieldElement.isSettingGroup) {

        val groupInfo =
                fieldElement.annotationMirrors.firstOrNull { types.isSameType(it.annotationType, settingGroupAnnType) } ?: continue

        val elementValues = elements.getElementValuesWithDefaults(groupInfo)

        val groupId = elementValues[settingGroupAnn.getAnnotationMethod("id")]!!.value as String
        val groupDesc = elementValues[settingGroupAnn.getAnnotationMethod("desc")]!!.value as String
        val groupGlobal = elementValues[settingGroupAnn.getAnnotationMethod("global")]!!.value as Boolean
        val groupOrder = elementValues[settingGroupAnn.getAnnotationMethod("order")]!!.value as Int

        groups.add(GroupInfo(groupId, groupDesc, groupGlobal, groupOrder, fieldElement))
      }

    }

    return FactoryInfo(groups, settings, factoryElement)
  }

  private val TypeMirror.isEnum: Boolean get() {
    return (this as? DeclaredType)?.asElement()?.kind == ElementKind.ENUM
  }

  private val VariableElement.isSetting: Boolean get() {
    return types.isSameType(types.erasure(asType()), types.erasure(settingType))
  }

  private val VariableElement.isSettingGroup: Boolean get() {
    return types.isSameType(asType(), settingGroupType)
  }

  private fun TypeElement.getAnnotationMethod(name: String): ExecutableElement {
    return enclosedElements
       .filter { it.kind == ElementKind.METHOD }
       .firstOrNull { it.simpleName.contentEquals(name) } as ExecutableElement
  }

  private val TypeMirror.isEnumUpperSnakeCase: Boolean get() {
    return (this as DeclaredType).asElement().enclosedElements
       .filter { it.kind == ElementKind.ENUM_CONSTANT }
       .all { it.simpleName.toString().matches("""[A-Z_]+""".toRegex()) }
  }

}
