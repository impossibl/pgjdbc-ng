package com.impossibl.jdbc.spy

import com.squareup.javapoet.*
import java.io.File
import java.sql.*
import javax.lang.model.element.Modifier
import javax.sql.*


fun main(args: Array<String>) {
  if (args.size > 1) throw IllegalArgumentException("Output directory is only allowed argument")
  val outputDir = if (args.isNotEmpty()) File(args[0]) else File(".")
  SpyGen().generate(outputDir)
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

  val targetPkg = "com.impossibl.jdbc.spy"
  val relayIfaceName = ClassName.get(targetPkg, "Relay")
  val traceOutputIfaceName = ClassName.get(targetPkg, "TraceOutput")
  val traceTypeName = ClassName.get(targetPkg, "Trace")
  val traceBuilderTypeName = traceTypeName.nestedClass("Builder")

  val targetIfaces = listOf<Class<*>>(
     Connection::class.java, DatabaseMetaData::class.java,
     Statement::class.java, PreparedStatement::class.java, CallableStatement::class.java, ParameterMetaData::class.java,
     ResultSet::class.java, ResultSetMetaData::class.java,
     java.sql.Array::class.java, Blob::class.java, Clob::class.java, NClob::class.java, Ref::class.java,
     RowId::class.java, Savepoint::class.java, SQLXML::class.java, Struct::class.java,
     DataSource::class.java,
     ConnectionPoolDataSource::class.java, PooledConnection::class.java,
     XADataSource::class.java, XAConnection::class.java
  )

  fun generate(outputDir: File) {

    for (ifaceClass in targetIfaces) {
      generate(ifaceClass, outputDir)
    }
  }

  private fun generate(targetIface: Class<*>, outputDir: File) {

    val targetIfaceNames = targetIfaces.map { ClassName.get(it) }.toSet()

    val targetIfaceName = ClassName.get(targetIface)
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

      val targetMethodTypeVarNames = targetMethod.typeParameters.map { TypeVariableName.get(it) }
      listenerSuccessMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      listenerFailureMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      tracerSuccessMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      tracerFailureMethodBldr.addTypeVariables(targetMethodTypeVarNames)
      relayMethodBldr.addTypeVariables(targetMethodTypeVarNames)

      val targetReturnTypeName = ClassName.get(targetMethod.genericReturnType)
      relayMethodBldr.returns(targetReturnTypeName)

      if (targetReturnTypeName != TypeName.VOID) {
        listenerSuccessMethodBldr.addParameter(targetReturnTypeName, "targetSuccessResult")
        tracerSuccessMethodBldr.addParameter(targetReturnTypeName, "result")
      }
      listenerFailureMethodBldr.addParameter(ClassName.get(Throwable::class.java), "targetFailureCause")
      tracerFailureMethodBldr.addParameter(ClassName.get(Throwable::class.java), "cause")

      for (parameter in targetMethod.parameters) {
        listenerSuccessMethodBldr.addParameter(parameter.parameterizedType, parameter.name)
        listenerFailureMethodBldr.addParameter(parameter.parameterizedType, parameter.name)
        tracerSuccessMethodBldr.addParameter(parameter.parameterizedType, parameter.name)
        tracerFailureMethodBldr.addParameter(parameter.parameterizedType, parameter.name)
        relayMethodBldr.addParameter(parameter.parameterizedType, parameter.name)
      }

      tracerSuccessMethodBldr.addCode("\$[trace(new \$T(\$S, \$S)\n", traceBuilderTypeName, targetIface.simpleName, targetMethod.name)
      tracerFailureMethodBldr.addCode("\$[trace(new \$T(\$S, \$S)\n", traceBuilderTypeName, targetIface.simpleName, targetMethod.name)
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

      relayMethodBldr.addExceptions(targetMethod.genericExceptionTypes.map { ClassName.get(it) })

      for (parameter in targetMethod.parameters) {
        val parameterType = parameter.parameterizedType
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

      val targetMethodParameterNames = targetMethod.parameters.joinToString { it.name }

      if (targetReturnTypeName == TypeName.VOID) {
        relayMethodBldr.addStatement("this.target.\$N(\$L)", targetMethod.name, targetMethodParameterNames)
        relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, targetMethodParameterNames)
      }
      else {

        relayMethodBldr.addStatement("\$T targetSuccessResult = this.target.\$N(\$L)", targetReturnTypeName, targetMethod.name, targetMethodParameterNames)

        relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, (listOf("targetSuccessResult") + targetMethod.parameters.map { it.name }).joinToString())

        if (targetReturnTypeName is ClassName && targetIfaceNames.contains(targetReturnTypeName)) {

          val resultRelayTypeName = getSpyClassName(targetReturnTypeName, ClassKind.Relay)
          requiredFactoryMethodNames.add(targetReturnTypeName)

          relayMethodBldr.addStatement("targetSuccessResult = targetSuccessResult != null ? new \$T(targetSuccessResult, this.listener.new\$LListener()) : null;", resultRelayTypeName, targetReturnTypeName.simpleName())
        }

        relayMethodBldr.addStatement("return targetSuccessResult")
      }

      val exceptions =
         if (targetMethod.genericExceptionTypes.isNotEmpty())
           CodeBlock.join(targetMethod.genericExceptionTypes.map { CodeBlock.of("\$T", ClassName.get(it)) }, " | ")
         else
           CodeBlock.of("\$T", ClassName.get(Throwable::class.java))

      relayMethodBldr.addCode("$<\n}\ncatch(\$L targetFailureCause) {$>\n", exceptions)

      relayMethodBldr.addStatement("this.listener.\$N(\$L)", targetMethod.name, (listOf("targetFailureCause") + targetMethod.parameters.map { it.name }).joinToString())
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

    JavaFile.builder(targetPkg, listenerIfaceBldr.build())
       .skipJavaLangImports(true)
       .build()
       .writeTo(outputDir)

    JavaFile.builder(targetPkg, tracerClassBldr.build())
       .skipJavaLangImports(true)
       .build()
       .writeTo(outputDir)

    JavaFile.builder(targetPkg, relayClassBldr.build())
       .skipJavaLangImports(true)
       .build()
       .writeTo(outputDir)
  }

}
