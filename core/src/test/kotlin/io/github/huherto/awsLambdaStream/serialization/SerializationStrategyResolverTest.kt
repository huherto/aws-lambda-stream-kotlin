package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertTrue

class SerializationStrategyResolverTest {

    private val envConfig = mockk<EnvironmentConfig>()

    @Test
    fun `explicit Jackson config selects Jackson`() {
        val resolver = SerializationStrategyResolver(
            config = SerializationConfig(strategy = SerializationStrategyKind.JACKSON)
        )
        val strategy = resolver.resolve()
        assertTrue(strategy is JacksonSerializationStrategy)
    }

    @Test
    fun `explicit kotlinx config selects kotlinx`() {
        val resolver = SerializationStrategyResolver(
            config = SerializationConfig(strategy = SerializationStrategyKind.KOTLINX)
        )
        val strategy = resolver.resolve()
        assertTrue(strategy is KotlinxSerializationStrategy)
    }

    @Test
    fun `env variable selects Jackson`() {
        every { envConfig.serializationStrategy() } returns "jackson"
        val resolver = SerializationStrategyResolver(envConfig = envConfig)
        val strategy = resolver.resolve()
        assertTrue(strategy is JacksonSerializationStrategy)
    }

    @Test
    fun `env variable selects kotlinx`() {
        every { envConfig.serializationStrategy() } returns "kotlinx"
        val resolver = SerializationStrategyResolver(envConfig = envConfig)
        val strategy = resolver.resolve()
        assertTrue(strategy is KotlinxSerializationStrategy)
    }

    @Test
    fun `explicit Moshi config fails because it is not implemented`() {
        val resolver = SerializationStrategyResolver(
            config = SerializationConfig(strategy = SerializationStrategyKind.MOSHI)
        )
        assertThrows<UnsupportedOperationException> {
            resolver.resolve()
        }
    }
}
