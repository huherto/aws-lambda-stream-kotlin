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
    ReflectionStrategy.copyCommonFields(from, to)
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
 * source properties with matching names. Unlike [copyCommonFields], this final property copyEvent
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
    return ReflectionStrategy.createFromCommonValues(from, targetClass, factory)
}

/**
 * Creates a copy of [from] with the provided overrides.
 *
 * This function uses the primary constructor of [from]'s class. Any overrides that match
 * constructor parameter names will be used instead of the values from [from].
 *
 * @param from The source object to copy.
 * @param overrides A map of property names to new values.
 * @return A new instance of the same class as [from].
 */
fun <T : Any> copyWithOverrides(from: T, overrides: Map<String, Any?>): T {
    return ReflectionStrategy.copyWithOverrides(from, overrides)
}

private object ReflectionStrategy {
    private val isKotlinReflectAvailable: Boolean by lazy {
        try {
            Class.forName("kotlin.reflect.full.KClasses")
            true
        } catch (e: Exception) {
            false
        }
    }

    private fun checkAvailable() {
        if (!isKotlinReflectAvailable) {
            throw IllegalStateException(
                "Kotlin reflection (kotlin-reflect.jar) is required for this operation. " +
                "Please add 'org.jetbrains.kotlin:kotlin-reflect' to your runtime dependencies."
            )
        }
    }

    fun copyCommonFields(from: Any, to: Any) {
        checkAvailable()
        KotlinReflectionImpl.copyCommonFields(from, to)
    }

    fun <T : Any> createFromCommonValues(
        from: Any,
        targetClass: KClass<out T>,
        factory: (() -> T)? = null
    ): T {
        if (factory != null) {
            val instance = factory()
            copyMutableProperties(from, instance)
            return instance
        }
        checkAvailable()
        val instance = KotlinReflectionImpl.createFromCommonValues(from, targetClass)
        copyMutableProperties(from, instance)
        return instance
    }

    fun <T : Any> copyWithOverrides(from: T, overrides: Map<String, Any?>): T {
        checkAvailable()
        val instance = KotlinReflectionImpl.copyWithOverrides(from, overrides)
        copyMutableProperties(from, instance)
        return instance
    }

    private fun copyMutableProperties(from: Any, to: Any) {
        if (isKotlinReflectAvailable) {
            KotlinReflectionImpl.copyMutableKotlinProperties(from, to)
        }
    }
}

/**
 * Isolated implementation that uses kotlin-reflect extension functions.
 * This object will only be loaded if kotlin-reflect is present on the classpath.
 */
private object KotlinReflectionImpl {

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
        targetClass: KClass<out T>
    ): T {
        val ctor = targetClass.primaryConstructor

        return when {
            ctor != null && ctor.visibility == KVisibility.PUBLIC -> instantiateFromPrimaryConstructor(from, targetClass)
            else -> throw IllegalArgumentException(
                "Target class ${targetClass.qualifiedName} must have a public primary constructor or a factory"
            )
        }
    }

    fun <T : Any> copyWithOverrides(from: T, overrides: Map<String, Any?>): T {
        val targetClass = from::class
        val ctor = targetClass.primaryConstructor
            ?: throw IllegalArgumentException("Class ${targetClass.qualifiedName} must have a primary constructor")

        val sourceProps = targetClass.memberProperties.associateBy { it.name }
        val args = mutableMapOf<KParameter, Any?>()

        for (param in ctor.parameters) {
            val name = param.name ?: continue
            if (overrides.containsKey(name)) {
                val value = overrides[name]
                if (value == null && !param.type.isMarkedNullable) continue
                args[param] = value
            } else {
                val sourceProp = sourceProps[name] ?: continue
                sourceProp.isAccessible = true
                val value = sourceProp.getter.call(from)
                if (value == null && !param.type.isMarkedNullable) continue
                args[param] = value
            }
        }

        return ctor.callBy(args)
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

    fun copyMutableKotlinProperties(from: Any, to: Any) {
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
}
