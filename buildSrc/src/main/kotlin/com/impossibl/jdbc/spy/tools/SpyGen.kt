package com.impossibl.jdbc.spy.tools

import com.github.javaparser.ParserConfiguration
import com.github.javaparser.resolution.declarations.*
import com.github.javaparser.resolution.types.*
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver
import com.squareup.javapoet.*
import java.io.File
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystems
import java.nio.file.Paths
import java.sql.*
import java.util.*
import javax.lang.model.element.Modifier
import javax.sql.*


fun main(args: Array<String>) {
  if (args.size > 1) throw IllegalArgumentException("Output directory is only allowed argument")
  val outputDir = if (args.isNotEmpty()) File(args[0]) else File(".")
  SpyGen().generateTo(outputDir)
}

class SpyGen {

  enum class ClassKind {
    Relay,
    Listener,
    Tracer
  }

  private fun getSpyClassName(typeName: ClassName, classKind: ClassKind): ClassName {
    return ClassName.get(targetPkg, typeName.simpleName() + classKind.name)
  }

  private val targetPkg = "com.impossibl.jdbc.spy"
  private val relayIfaceName = ClassName.get(targetPkg, "Relay")!!
  private val traceOutputIfaceName = ClassName.get(targetPkg, "TraceOutput")!!
  private val traceTypeName = ClassName.get(targetPkg, "Trace")!!
  private val traceBuilderTypeName = traceTypeName.nestedClass("Builder")!!
  private val targetIfaces = listOf<Class<*>>(
     Connection::class.java, DatabaseMetaData::class.java,
     Statement::class.java, PreparedStatement::class.java, CallableStatement::class.java, ParameterMetaData::class.java,
     ResultSet::class.java, ResultSetMetaData::class.java,
     java.sql.Array::class.java, Blob::class.java, Clob::class.java, NClob::class.java, Ref::class.java,
     RowId::class.java, Savepoint::class.java, SQLXML::class.java, Struct::class.java,
     DataSource::class.java,
     ConnectionPoolDataSource::class.java, PooledConnection::class.java,
     XADataSource::class.java, XAConnection::class.java
  )
  private val solver = CombinedTypeSolver(
     PathJavaParserTypeSolver(
        SpyGen::class.java.getResource("/jdbc-api").toURI().let {
          if (it.scheme.startsWith("jar")) {
            try { FileSystems.newFileSystem(it, mapOf<String, Any>()) } catch (x: FileSystemAlreadyExistsException) {}
          }
          Paths.get(it)
        },
        ParserConfiguration().setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_8)
     ),
     ReflectionTypeSolver(true)
  )

  fun generate(): List<JavaFile> = targetIfaces.flatMap { generate(it) }

  fun generateTo(outputDir: File) {

    return generate().forEach { it.writeTo(outputDir) }
  }

  private fun generate(targetIfaceIn: Class<*>): List<JavaFile> {

    val targetIface = solver.tryToSolveType(targetIfaceIn.name)
       .let {
         if (it.isSolved) {
           it.correspondingDeclaration
         }
         else {
           throw IllegalArgumentException("No source class found for ${targetIfaceIn.canonicalName}")
         }
       }

    val targetIfaceNames = targetIfaces.map { ClassName.get(it) }.toSet()

    val targetIfaceName = targetIface.typeName
    val listenerIfaceName = getSpyClassName(targetIfaceName, ClassKind.Listener)
    val tracerClassName = getSpyClassName(targetIfaceName, ClassKind.Tracer)
    val relayClassName = getSpyClassName(targetIfaceName, ClassKind.Relay)

    val listenerIfaceBldr = TypeSpec.interfaceBuilder(listenerIfaceName)

    val tracerClassBldr = TypeSpec.classBuilder(tracerClassName)
       .addModifiers(Modifier.PUBLIC)
       .addSuperinterface(listenerIfaceName)
       .addField(traceOutputIfaceName, "out")
       .addMethod(
          MethodSpec.constructorBuilder()
             .addModifiers(Modifier.PUBLIC)
             .addParameter(traceOutputIfaceName, "out")
             .addStatement("this.out = out")
             .build()
       )
       .addMethod(
          MethodSpec.methodBuilder("trace")
             .addModifiers(Modifier.PUBLIC)
             .addParameter(traceTypeName, "trace")
             .addStatement("this.out.trace(trace)")
             .build()
       )

    val relayClassBldr =
       TypeSpec.classBuilder(relayClassName)
          .addModifiers(Modifier.PUBLIC)
          .addSuperinterface(ParameterizedTypeName.get(relayIfaceName, targetIfaceName))
          .addSuperinterface(targetIfaceName)

    relayClassBldr.addField(
       FieldSpec.builder(targetIfaceName, "target", Modifier.PUBLIC)
          .build()
    )
    relayClassBldr.addField(
       FieldSpec.builder(listenerIfaceName, "listener", Modifier.PUBLIC)
          .build()
    )

    relayClassBldr.addMethod(
       MethodSpec.constructorBuilder()
          .addModifiers(Modifier.PUBLIC)
          .addParameter(targetIfaceName, "target")
          .addParameter(listenerIfaceName, "listener")
          .addStatement("this.target = target")
          .addStatement("this.listener = listener")
          .build()
    )

    relayClassBldr.addMethod(
       MethodSpec.methodBuilder("getTarget")
          .addModifiers(Modifier.PUBLIC)
          .returns(targetIfaceName)
          .addStatement("return target")
          .build()
    )

    val requiredFactoryMethodNames = mutableSetOf<ClassName>()

    for (targetMethod in targetIface.methods) {

      val listenerSuccessMethodBldr = MethodSpec.methodBuilder(targetMethod.name)
         .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
      val listenerFailureMethodBldr = MethodSpec.methodBuilder(targetMethod.name)
         .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)

      val tracerSuccessMethodBldr = MethodSpec.methodBuilder(targetMethod.name)
         .addModifiers(Modifier.PUBLIC)
      val tracerFailureMethodBldr = MethodSpec.methodBuilder(targetMethod.name)
         .addModifiers(Modifier.PUBLIC)

      val relayMethodBldr = MethodSpec.methodBuilder(targetMethod.name)
         .addModifiers(Modifier.PUBLIC)

      if (targetMethod.toAst().isPresent) {
        if (targetMethod.toAst().get().isAnnotationPresent(java.lang.Deprecated::class.java)) {

          relayMethodBldr.addAnnotation(
             AnnotationSpec.builder(SuppressWarnings::class.java)
                .addMember("value", "\$S", "deprecation")
                .build()
          )
        }
      }

      val targetMethodTypeVarNames = targetMethod.typeParameters.map { it.typeVariableName }
      listenerSuccessMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      listenerFailureMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      tracerSuccessMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      tracerFailureMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      relayMethodBldr.addTypeVariables(targetMethodTypeVarNames)

      val targetReturnTypeName = targetMethod.returnType.typeName
      relayMethodBldr.returns(targetReturnTypeName)

      if (targetReturnTypeName != TypeName.VOID) {
        listenerSuccessMethodBldr.addParameter(targetReturnTypeName, "targetSuccessResult")
        tracerSuccessMethodBldr.addParameter(targetReturnTypeName, "result")
      }
      listenerFailureMethodBldr.addParameter(ClassName.get(Throwable::class.java), "targetFailureCause")
      tracerFailureMethodBldr.addParameter(ClassName.get(Throwable::class.java), "cause")

      for (parameter in targetMethod.parameters) {
        listenerSuccessMethodBldr.addParameter(parameter.type.typeName, parameter.name)
        listenerFailureMethodBldr.addParameter(parameter.type.typeName, parameter.name)
        tracerSuccessMethodBldr.addParameter(parameter.type.typeName, parameter.name)
        tracerFailureMethodBldr.addParameter(parameter.type.typeName, parameter.name)
        relayMethodBldr.addParameter(parameter.type.typeName, parameter.name)
      }

      tracerSuccessMethodBldr.addCode("\$[trace(new \$T(\$S, \$S)\n", traceBuilderTypeName, targetIface.name, targetMethod.name)
      tracerFailureMethodBldr.addCode("\$[trace(new \$T(\$S, \$S)\n", traceBuilderTypeName, targetIface.name, targetMethod.name)
      targetMethod.parameters.forEach {
        tracerSuccessMethodBldr.addCode(".withParameter(\$S, \$L)\n", it.name, it.name)
        tracerFailureMethodBldr.addCode(".withParameter(\$S, \$L)\n", it.name, it.name)
      }

      if (targetReturnTypeName != TypeName.VOID) {
        tracerSuccessMethodBldr.addCode(".returned(result)\n.build());\$]\n")
      }
      else {
        tracerSuccessMethodBldr.addCode(".returned()\n.build());\$]\n")
      }

      tracerFailureMethodBldr.addCode(".threw(cause)\n.build());\$]\n")

      relayMethodBldr.addExceptions(targetMethod.specifiedExceptions.map { it.typeName })

      for (parameter in targetMethod.parameters) {
        val parameterType = parameter.type.typeName
        // unwrap parameter that are spy instances... we only want to see the user's use of the API
        if (parameterType is Class<*> && (parameterType == Object::class.java || targetIfaces.any { it.isAssignableFrom(parameterType) })) {
          val relayTypeName =
             if (parameterType == Object::class.java)
               relayIfaceName
             else
               getSpyClassName(ClassName.get(parameterType), ClassKind.Relay)
          relayMethodBldr.addCode(CodeBlock.builder().addNamed(
             "\$[\$name:L = (\$name:L instanceof \$type:T) ? ((\$type:T)\$name:L).getTarget() : \$name:L;\$]\n",
             mapOf("name" to parameter.name, "type" to relayTypeName)
          ).build())
        }
      }

      relayMethodBldr.addCode("try {$>\n")

      val targetMethodParameterNames = targetMethod.parameters.map { it.name }

      if (targetReturnTypeName == TypeName.VOID) {
        relayMethodBldr.addStatement("this.target.\$N(\$L)", targetMethod.name, targetMethodParameterNames.joinToString())
        relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, targetMethodParameterNames.joinToString())
      }
      else {

        relayMethodBldr.addStatement("\$T targetSuccessResult = this.target.\$N(\$L)", targetReturnTypeName, targetMethod.name, targetMethodParameterNames.joinToString())

        relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, (listOf("targetSuccessResult") + targetMethodParameterNames).joinToString())

        if (targetReturnTypeName is ClassName && targetIfaceNames.contains(targetReturnTypeName)) {

          val resultRelayTypeName = getSpyClassName(targetReturnTypeName, ClassKind.Relay)
          requiredFactoryMethodNames.add(targetReturnTypeName)

          relayMethodBldr.addStatement("targetSuccessResult = targetSuccessResult != null ? new \$T(targetSuccessResult, this.listener.new\$LListener()) : null", resultRelayTypeName, targetReturnTypeName.simpleName())
        }

        relayMethodBldr.addStatement("return targetSuccessResult")
      }

      val exceptions =
         if (targetMethod.specifiedExceptions.isNotEmpty())
           CodeBlock.join(targetMethod.specifiedExceptions.map { CodeBlock.of("\$T", it.typeName) }, " | ")
         else
           CodeBlock.of("\$T", ClassName.get(Throwable::class.java))

      relayMethodBldr.addCode("$<\n}\ncatch(\$L targetFailureCause) {$>\n", exceptions)

      relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, (listOf("targetFailureCause") + targetMethodParameterNames).joinToString())
      relayMethodBldr.addStatement("throw targetFailureCause")

      relayMethodBldr.addCode("$<\n}\n")

      listenerIfaceBldr.addMethod(listenerSuccessMethodBldr.build())
      listenerIfaceBldr.addMethod(listenerFailureMethodBldr.build())
      tracerClassBldr.addMethod(tracerSuccessMethodBldr.build())
      tracerClassBldr.addMethod(tracerFailureMethodBldr.build())
      relayClassBldr.addMethod(relayMethodBldr.build())
    }

    for (factoryMethodTypeName in requiredFactoryMethodNames) {
      listenerIfaceBldr.addMethod(
         MethodSpec.methodBuilder("new${factoryMethodTypeName.simpleName().capitalize()}Listener")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(getSpyClassName(factoryMethodTypeName, ClassKind.Listener))
            .build()
      )

      tracerClassBldr.addMethod(
         MethodSpec.methodBuilder("new${factoryMethodTypeName.simpleName().capitalize()}Listener")
            .addModifiers(Modifier.PUBLIC)
            .returns(getSpyClassName(factoryMethodTypeName, ClassKind.Listener))
            .addStatement("return new \$T(out)", getSpyClassName(factoryMethodTypeName, ClassKind.Tracer))
            .build()
      )
    }

    val listenerIface =
       JavaFile.builder(targetPkg, listenerIfaceBldr.build())
          .skipJavaLangImports(true)
          .build()

    val tracerClass =
       JavaFile.builder(targetPkg, tracerClassBldr.build())
        .skipJavaLangImports(true)
        .build()

    val relayClass =
       JavaFile.builder(targetPkg, relayClassBldr.build())
        .skipJavaLangImports(true)
        .build()

    return listOf(listenerIface, tracerClass, relayClass)
  }

}

val ResolvedTypeParameterDeclaration.typeVariableName: TypeVariableName
  get() =
     TypeVariableName.get(this.name, *this.bounds.map { it.type.typeName }.toTypedArray())

val ResolvedType.typeName: TypeName get() =
  when {
    this is ResolvedPrimitiveType -> ClassName.bestGuess(this.boxTypeQName).unbox()
    this is ResolvedArrayType -> ArrayTypeName.of(this.componentType.typeName)
    this is ResolvedVoidType -> TypeName.VOID
    this is ResolvedWildcard -> this.wildcardTypeName
    this is ResolvedTypeVariable -> TypeVariableName.get(this.describe())
    this is ResolvedReferenceType ->
      if (this.typeParametersMap.isEmpty())
        this.typeDeclaration.typeName
      else
        ParameterizedTypeName.get(this.typeDeclaration.typeName, *this.typeParametersValues().map { it.typeName }.toTypedArray())
    else -> throw IllegalArgumentException("Unsupported ResolvedType: ${this.javaClass}")
  }

val ResolvedWildcard.wildcardTypeName: WildcardTypeName
  get() =
    when {
      isUpperBounded -> WildcardTypeName.supertypeOf(boundedType.typeName)
      isLowerBounded -> WildcardTypeName.subtypeOf(boundedType.typeName)
      else -> WildcardTypeName.subtypeOf(Object::class.java)
    }

val ResolvedReferenceTypeDeclaration.typeName: ClassName get() {
  val simpleNames = this.className.split('.')
  return ClassName.get(this.packageName, simpleNames.first(), *simpleNames.drop(1).toTypedArray())
}

val ResolvedMethodLikeDeclaration.parameters: List<ResolvedParameterDeclaration> get() {
  val params = mutableListOf<ResolvedParameterDeclaration>()
  for (i in 0 until this.numberOfParams) {
    params.add(this.getParam(i))
  }
  return params
}

val ResolvedReferenceTypeDeclaration.methods: Set<ResolvedMethodDeclaration> get() {
  val methods = HashSet<ResolvedMethodDeclaration>()

  val methodsSignatures = HashSet<String>()

  for (methodDeclaration in declaredMethods) {
    methods.add(methodDeclaration)
    methodsSignatures.add(methodDeclaration.signature)
  }

  for (ancestor in allAncestors) {
    if (ancestor.qualifiedName == "java.lang.Object") continue
    for (mu in ancestor.declaredMethods) {
      val signature = mu.declaration.signature
      if (!methodsSignatures.contains(signature)) {
        methodsSignatures.add(signature)
        methods.add(mu.declaration)
      }
    }
  }

  return methods
}
