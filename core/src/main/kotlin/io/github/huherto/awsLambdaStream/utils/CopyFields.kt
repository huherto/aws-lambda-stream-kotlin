package io.github.huherto.awsLambdaStream.utils

import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.KVisibility
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

/**
 * Copies non-null property values from one object to mutable properties on another object
 * when the properties share the same name.
 *
 * This function uses Kotlin reflection to inspect both objects. For every property on [to],
 * it looks for a property with the same name on [from]. If the source value is non-null and
 * the target property is mutable, the value is assigned to the target property.
 *
 * Private or otherwise non-public properties can be read or written because the reflected
 * properties are marked accessible.
 *
 * Properties are matched by name only. Type compatibility is not checked before assignment,
 * so incompatible source and target property types may cause a reflection exception at runtime.
 *
 * @param from The source object to read property values from.
 * @param to The target object whose mutable properties should be updated.
 */
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

/**
 * Creates an instance of [targetClass] and initializes it with values copied from [from].
 *
 * The target instance is created in one of two ways:
 *
 * 1. If [factory] is provided, the factory is called and its result is used.
 * 2. Otherwise, [targetClass] must have a public primary constructor. Constructor parameters
 *    are populated from source properties with matching names.
 *
 * After construction, mutable Kotlin properties on the created instance are also updated from
 * source properties with matching names. Unlike [copyCommonFields], this final property copy
 * includes null values.
 *
 * Constructor arguments are matched by parameter name. If a matching source property contains
 * `null` for a non-nullable constructor parameter, that parameter is skipped so the constructor
 * default value can be used when available.
 *
 * @param from The source object to read constructor arguments and property values from.
 * @param targetClass The class to instantiate.
 * @param factory Optional factory used to create the target instance instead of using the
 * public primary constructor.
 * @return A new instance of [targetClass] initialized from common source values.
 * @throws IllegalArgumentException If [factory] is not provided and [targetClass] does not have
 * a public primary constructor.
 */
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

/**
 * Instantiates [targetClass] by calling its primary constructor with values from [from].
 *
 * Constructor parameters are matched to source properties by name. Only matching source
 * properties are included in the constructor argument map.
 *
 * If a source value is `null` and the matching constructor parameter is not nullable, the
 * argument is omitted. This allows `callBy` to use the constructor parameter's default value,
 * if one exists.
 *
 * @param from The source object to read constructor argument values from.
 * @param targetClass The target class to instantiate.
 * @return A new instance of [targetClass].
 * @throws IllegalArgumentException If [targetClass] does not have a primary constructor.
 */
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

/**
 * Copies property values from [from] to mutable Kotlin properties on [to] when property names match.
 *
 * This function copies both null and non-null values. It only writes to properties that are
 * represented as [KMutableProperty1]. Read-only target properties are ignored.
 *
 * Private or otherwise non-public properties can be read or written because the reflected
 * properties are marked accessible.
 *
 * Properties are matched by name only. Type compatibility is not checked before assignment,
 *so incompatible source and target property types may cause a reflection exception at runtime.
 *
 * @param from The source object to read property values from.
 * @param to The target object whose mutable properties should be updated.
 */
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