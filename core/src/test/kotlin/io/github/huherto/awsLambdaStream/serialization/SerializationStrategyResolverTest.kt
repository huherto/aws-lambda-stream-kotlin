package io.github.huherto.awsLambdaStream.serialization

import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class SerializationStrategyResolverTest {

    @Test
    fun `resolves to kotlinx strategy`() {
        val resolver = SerializationStrategyResolver()
        val strategy = resolver.resolve()
        assertTrue(strategy is KotlinxSerializationStrategy)
    }
}
