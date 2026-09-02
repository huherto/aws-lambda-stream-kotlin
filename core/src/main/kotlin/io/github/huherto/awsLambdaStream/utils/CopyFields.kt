package io.github.huherto.awsLambdaStream.utils

import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.KVisibility
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

fun copyCommonFields(from: Any, to: Any) {
    ReflectionStrategy.copyCommonFields(from, to)
}

fun <T : Any> createFromCommonValues(
    from: Any,
    targetClass: KClass<out T>,
    factory: (() -> T)? = null
): T {
    return ReflectionStrategy.createFromCommonValues(from, targetClass, factory)
}

fun <T : Any> copyWithOverrides(from: T, overrides: Map<String, Any?>): T {
    return ReflectionStrategy.copyWithOverrides(from, overrides)
}

/** Strategy for copying fields using reflection. */
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

/** Implementation using kotlin-reflect. */
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
