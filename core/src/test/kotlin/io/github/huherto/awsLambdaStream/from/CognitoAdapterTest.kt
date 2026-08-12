package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.JsonRaw
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class CognitoAdapterTest {

    private val envConfig = spyk(EnvironmentConfig()).apply {
        every { serializationStrategy() } returns "jackson"
    }

    private fun faultManager() = FaultManager(
        envConfig = envConfig,
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )

    private fun adapter() = CognitoAdapter(faultManager())

    @Test
    fun `fromCognito should map cognito event fields`() = runBlocking {
        val adapter = adapter()
        val cognitoEvent = mapOf(
            "triggerSource" to "PostConfirmation_ConfirmSignUp",
            "userName" to "user-123",
            "region" to "us-east-1",
            "userPoolId" to "us-east-1_abc123",
            "request" to mapOf(
                "userAttributes" to mapOf("email" to "user@example.com")
            )
        )

        val results = adapter.fromCognito(cognitoEvent).toList()

        results.size shouldBe 1
        val event = results[0].event as CognitoEvent
        event.id shouldNotBe null
        event.eventType() shouldBe "aws-cognito-post-confirmation-confirm-sign-up"
        event.partitionKey shouldBe "user-123"
        event.tags shouldBe mapOf(
            "region" to "us-east-1",
            "source" to "us-east-1_abc123"
        )
        val raw = event.raw as JsonRaw
        raw.value.toString() shouldBe """{"userAttributes":{"email":"user@example.com"}}"""
    }
}
