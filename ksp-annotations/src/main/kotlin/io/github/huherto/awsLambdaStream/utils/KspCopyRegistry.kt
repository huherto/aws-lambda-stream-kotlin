package io.github.huherto.awsLambdaStream.utils

import kotlin.reflect.KClass

/**
 * Interface for generated KSP helpers that perform reflection-free copying.
 */
interface KspHelper<T> {
    fun copyWithOverrides(instance: T, overrides: Map<String, Any?>): T
}

/**
 * Registry for KSP helpers.
 */
object KspCopyRegistry {
    private val helpers = mutableMapOf<String, KspHelper<*>>()

    fun register(className: String, helper: KspHelper<*>) {
        helpers[className] = helper
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getHelper(clazz: KClass<out T>): KspHelper<T>? {
        return helpers[clazz.qualifiedName] as? KspHelper<T>
    }
}
