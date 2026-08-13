package io.github.huherto.awsLambdaStream.ksp

import com.google.devtools.ksp.processing.*
import com.google.devtools.ksp.symbol.*
import com.google.devtools.ksp.validate
import io.github.huherto.awsLambdaStream.utils.Copyable
import java.io.PrintWriter

class CopyableProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger
) : SymbolProcessor {

    private val annotatedClasses = mutableListOf<KSClassDeclaration>()

    override fun process(resolver: Resolver): List<KSAnnotated> {
        val symbols = resolver.getSymbolsWithAnnotation(Copyable::class.qualifiedName!!)
        val ret = symbols.filter { !it.validate() }.toList()
        symbols
            .filter { it is KSClassDeclaration && it.validate() }
            .forEach {
                val clazz = it as KSClassDeclaration
                annotatedClasses.add(clazz)
                clazz.accept(CopyableVisitor(), Unit)
            }
        return ret
    }

    override fun finish() {
        if (annotatedClasses.isEmpty()) return

        val firstClass = annotatedClasses.first()
        val packageName = firstClass.packageName.asString()
        val registrarName = "KspRegistrar"

        val file = codeGenerator.createNewFile(
            Dependencies(true, *annotatedClasses.mapNotNull { it.containingFile }.toTypedArray()),
            packageName,
            registrarName
        )

        PrintWriter(file).use { writer ->
            writer.println("package $packageName")
            writer.println()
            writer.println("import io.github.huherto.awsLambdaStream.utils.KspCopyRegistry")
            writer.println()
            writer.println("object $registrarName {")
            writer.println("    fun registerAll() {")
            annotatedClasses.forEach { clazz ->
                val pName = clazz.packageName.asString()
                val cName = clazz.simpleName.asString()
                writer.println("        KspCopyRegistry.register(\"$pName.$cName\", ${pName}.${cName}Helper)")
            }
            writer.println("    }")
            writer.println("}")
        }
    }

    inner class CopyableVisitor : KSVisitorVoid() {
        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            val packageName = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val file = codeGenerator.createNewFile(
                Dependencies(true, classDeclaration.containingFile!!),
                packageName,
                "${className}Helper"
            )

            val primaryConstructor = classDeclaration.primaryConstructor
            val parameters = primaryConstructor?.parameters ?: emptyList()

            PrintWriter(file).use { writer ->
                writer.println("package $packageName")
                writer.println()
                writer.println("import io.github.huherto.awsLambdaStream.utils.KspHelper")
                writer.println()

                writer.println("object ${className}Helper : KspHelper<$className> {")
                writer.println("    override fun copyWithOverrides(instance: $className, overrides: Map<String, Any?>): $className {")
                
                if (classDeclaration.modifiers.contains(Modifier.DATA)) {
                    writer.println("        return instance.copy(")
                    parameters.forEachIndexed { index, param ->
                        val name = param.name?.asString() ?: return@forEachIndexed
                        val comma = if (index < parameters.size - 1) "," else ""
                        val type = param.type.resolve()
                        val typeName = type.toFullyQualifiedString()
                        writer.println("            $name = if (overrides.containsKey(\"$name\")) overrides[\"$name\"] as $typeName else instance.$name$comma")
                    }
                    writer.println("        )")
                } else {
                    writer.println("        return $className(")
                    parameters.forEachIndexed { index, param ->
                        val name = param.name?.asString() ?: return@forEachIndexed
                        val comma = if (index < parameters.size - 1) "," else ""
                        val type = param.type.resolve()
                        val typeName = type.toFullyQualifiedString()
                        writer.println("            $name = if (overrides.containsKey(\"$name\")) overrides[\"$name\"] as $typeName else instance.$name$comma")
                    }
                    writer.println("        )")
                }
                writer.println("    }")
                writer.println("}")
            }
        }

        private fun KSType.toFullyQualifiedString(): String {
            val declaration = this.declaration
            val baseName = declaration.qualifiedName?.asString() ?: this.toString()
            val typeArgs = if (this.arguments.isNotEmpty()) {
                this.arguments.joinToString(", ", "<", ">") { arg ->
                    arg.type?.resolve()?.toFullyQualifiedString() ?: arg.toString()
                }
            } else ""
            val nullable = if (this.isMarkedNullable) "?" else ""
            return "$baseName$typeArgs$nullable"
        }
    }
}

class CopyableProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return CopyableProcessor(environment.codeGenerator, environment.logger)
    }
}
