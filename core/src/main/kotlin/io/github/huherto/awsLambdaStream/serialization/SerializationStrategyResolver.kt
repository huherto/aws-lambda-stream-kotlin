package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry

class SerializationStrategyResolver(
    private val envConfig: EnvironmentConfig = GlobalRegistry.envConfig(),
    private val config: SerializationConfig = SerializationConfig()
) {
    fun resolve(): SerializationStrategy {
        val kind = getStrategyKind()
        return when (kind) {
            SerializationStrategyKind.JACKSON -> createJacksonStrategy()
            SerializationStrategyKind.KOTLINX -> createKotlinxStrategy()
            SerializationStrategyKind.MOSHI -> createMoshiStrategy()
            SerializationStrategyKind.AUTO -> resolveAuto()
        }
    }

    private fun getStrategyKind(): SerializationStrategyKind {
        if (config.strategy != SerializationStrategyKind.AUTO) {
            return config.strategy
        }

        val envValue = envConfig.serializationStrategy()

        return when (envValue?.lowercase()) {
            "jackson" -> SerializationStrategyKind.JACKSON
            "kotlinx" -> SerializationStrategyKind.KOTLINX
            "moshi" -> SerializationStrategyKind.MOSHI
            "auto" -> SerializationStrategyKind.AUTO
            null -> SerializationStrategyKind.AUTO
            else -> throw IllegalArgumentException("Unknown serialization strategy: $envValue")
        }
    }

    private fun resolveAuto(): SerializationStrategy {
        val available = mutableListOf<SerializationStrategyKind>()

        if (isJacksonAvailable()) {
            available.add(SerializationStrategyKind.JACKSON)
        }
        if (isKotlinxAvailable()) {
            available.add(SerializationStrategyKind.KOTLINX)
        }
        if (isMoshiAvailable()) {
            available.add(SerializationStrategyKind.MOSHI)
        }

        return when {
            available.isEmpty() -> throw IllegalStateException(
                "No serialization strategy detected. Please add Jackson or kotlinx-serialization to your classpath, or configure it explicitly."
            )
            available.size > 1 -> throw IllegalStateException(
                "Multiple serialization strategies detected ($available). Please configure one explicitly using SERIALIZATION_STRATEGY or SerializationConfig."
            )
            else -> when (available.first()) {
                SerializationStrategyKind.JACKSON -> createJacksonStrategy()
                SerializationStrategyKind.KOTLINX -> createKotlinxStrategy()
                SerializationStrategyKind.MOSHI -> createMoshiStrategy()
                else -> throw IllegalStateException("Unexpected strategy kind: ${available.first()}")
            }
        }
    }

    private fun isJacksonAvailable() = isClassAvailable("com.fasterxml.jackson.databind.ObjectMapper")
    private fun isKotlinxAvailable() = isClassAvailable("kotlinx.serialization.json.Json")
    private fun isMoshiAvailable() = isClassAvailable("com.squareup.moshi.Moshi")

    private fun isClassAvailable(className: String): Boolean {
        return try {
            Class.forName(className)
            true
        } catch (e: ClassNotFoundException) {
            false
        }
    }

    private fun createJacksonStrategy(): SerializationStrategy = JacksonSerializationStrategy()
    private fun createKotlinxStrategy(): SerializationStrategy = KotlinxSerializationStrategy()
    private fun createMoshiStrategy(): SerializationStrategy = throw UnsupportedOperationException("Moshi is not yet supported")
}
