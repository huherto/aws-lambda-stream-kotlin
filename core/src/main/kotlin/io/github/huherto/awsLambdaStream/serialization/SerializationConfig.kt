package io.github.huherto.awsLambdaStream.serialization

enum class SerializationStrategyKind {
    AUTO,
    JACKSON,
    KOTLINX,
    MOSHI,
}

data class SerializationConfig(
    val strategy: SerializationStrategyKind = SerializationStrategyKind.AUTO,
)
