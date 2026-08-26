package io.github.huherto.awsLambdaStream.serialization

class SerializationStrategyResolver(
    private val config: SerializationConfig = SerializationConfig()
) {
    fun resolve(): SerializationStrategy {
        return KotlinxSerializationStrategy()
    }
}
