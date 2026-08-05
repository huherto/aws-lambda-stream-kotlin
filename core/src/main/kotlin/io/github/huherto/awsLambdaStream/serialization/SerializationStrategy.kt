package io.github.huherto.awsLambdaStream.serialization

interface SerializationStrategy {
    fun serialize(value: Any?): String

    fun <T : Any> deserialize(
        payload: String,
        targetType: Class<T>,
    ): T
}
