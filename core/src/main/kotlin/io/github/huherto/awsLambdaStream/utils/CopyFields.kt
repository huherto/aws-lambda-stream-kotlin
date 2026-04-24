package io.github.huherto.awsLambdaStream.utils

import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.KVisibility
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

fun copyCommonFields(from: Any, to: Any) {
    val sourceProps = from::class.memberProperties.associateBy { it.name }

    for (targetProp in to::class.memberProperties) {
        val sourceProp = sourceProps[targetProp.name] ?: continue

        sourceProp.isAccessible = true
        targetProp.isAccessible = true

        val sourceValue = sourceProp.getter.call(from) ?: continue

        val targetMutable = targetProp as? KMutableProperty1<*, *>
        targetMutable?.setter?.call(to, sourceValue)
    }
}

fun <T : Any> createFromCommonValues(
    from: Any,
    targetClass: KClass<out T>,
    factory: (() -> T)? = null
): T {
    val ctor = targetClass.primaryConstructor

    val instance = when {
        factory != null -> factory()
        ctor != null && ctor.visibility == KVisibility.PUBLIC -> instantiateFromPrimaryConstructor(from, targetClass)
        else -> throw IllegalArgumentException(
            "Target class ${targetClass.qualifiedName} must have a public primary constructor or a factory"
        )
    }

    copyMutableKotlinProperties(from, instance)
    return instance
}

private fun <T : Any> instantiateFromPrimaryConstructor(
    from: Any,
    targetClass: KClass<T>
): T {
    val ctor = targetClass.primaryConstructor
        ?: throw IllegalArgumentException("Target class ${targetClass.qualifiedName} must have a primary constructor")

    val sourceProps = from::class.memberProperties.associateBy { it.name }
    val args = mutableMapOf<KParameter, Any?>()

    for (param in ctor.parameters) {
        val name = param.name ?: continue
        val sourceProp = sourceProps[name] ?: continue

        sourceProp.isAccessible = true
        val value = sourceProp.getter.call(from)

        if (value == null && !param.type.isMarkedNullable) continue
        args[param] = value
    }

    return ctor.callBy(args)
}

private fun copyMutableKotlinProperties(from: Any, to: Any) {
    val sourceProps = from::class.memberProperties.associateBy { it.name }

    for (targetProp in to::class.memberProperties) {
        val sourceProp = sourceProps[targetProp.name] ?: continue
        val targetMutable = targetProp as? KMutableProperty1<*, *> ?: continue

        sourceProp.isAccessible = true
        targetProp.isAccessible = true

        val value = sourceProp.getter.call(from)
        targetMutable.setter.call(to, value)
    }
}