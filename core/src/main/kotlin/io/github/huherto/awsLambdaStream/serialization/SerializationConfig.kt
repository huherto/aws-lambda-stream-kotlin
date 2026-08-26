package io.github.huherto.awsLambdaStream.serialization

enum class SerializationStrategyKind {
    AUTO,
    KOTLINX,
}

data class SerializationConfig(
    val strategy: SerializationStrategyKind = SerializationStrategyKind.AUTO,
)
