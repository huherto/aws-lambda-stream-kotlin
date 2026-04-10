package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.testsupport.EventBridgeClientFake
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlin.time.Duration.Companion.milliseconds

class EventBridgeConnectorTest : FunSpec({

    beforeTest {
        EventBridgeConnector.Companion.clearClients()
    }

    afterTest {
        unmockkAll()
    }

    fun envConfig() : EnvironmentConfig {
        return spyk(EnvironmentConfig())
    }

    test("should handle retry assertions and delay calculations") {
        // Arrange
        val eventBridgeClient = EventBridgeClientFake(EventBridgeClient{})
        val eventBridgeClientFactory = mockk<EventBridgeClientFactory>()
        every { eventBridgeClientFactory.createClient(any()) } returns eventBridgeClient
        val connector = EventBridgeConnector(
            pipelineId = "test-pipeline",
            envConfig = envConfig(),
            retryConfig = RetryConfig(),
            clientFactory = eventBridgeClientFactory,
            timeout = 1000.milliseconds)
        val baseDelay = 1000L

        // Act & Assert - Delay Calculations
        val delayAttempt1 = connector.getDelay(baseDelay, 1)
        val delayAttempt2 = connector.getDelay(baseDelay, 2)
        val delayAttempt3 = connector.getDelay(baseDelay, 3)

        delayAttempt1 shouldBe 1000L
        delayAttempt2 shouldBe 2000L
        delayAttempt3 shouldBe 4000L

        // Act & Assert - Retry Assertions
        connector.assertMaxRetries(3, 3) // Should not throw

        val exception = shouldThrow<IllegalStateException> {
            connector.assertMaxRetries(4, 3)
        }
        exception.message shouldBe "Maximum retry attempts exceeded."
    }

    test("should extract unprocessed entries and accumulate responses") {
        // Arrange
        val eventBridgeClient = EventBridgeClientFake(EventBridgeClient{})
        val eventBridgeClientFactory = mockk<EventBridgeClientFactory>()
        every { eventBridgeClientFactory.createClient(any()) } returns eventBridgeClient
        val connector = EventBridgeConnector(
            pipelineId = "test-pipeline",
            envConfig = envConfig(),
            retryConfig = RetryConfig(),
            clientFactory = eventBridgeClientFactory,
            timeout = 1000.milliseconds)
        
        val request = PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry { source = "source1" },
                PutEventsRequestEntry { source = "source2" }
            )
        }
        
        val responseAttempt1 = PutEventsResponse {
            failedEntryCount = 1
            entries = listOf(
                PutEventsResultEntry { eventId = "id-1" }, // Success
                PutEventsResultEntry { errorCode = "InternalFailure" } // Failed
            )
        }

        val responseAttempt2 = PutEventsResponse {
            failedEntryCount = 0
            entries = listOf(
                PutEventsResultEntry { eventId = "id-2" } // Success on retry
            )
        }

        // Act - Unprocessed Extract
        val unprocessedReq = connector.unprocessed(request, responseAttempt1)

        // Assert - Unprocessed Extract
        unprocessedReq.entries.shouldNotBeNull()
        unprocessedReq.entries?.size shouldBe 1
        unprocessedReq.entries?.first()?.source shouldBe "source2"

        // Act - Accumulate Responses
        val attempts = listOf(responseAttempt1)
        val finalResponse = connector.accumulate(attempts, responseAttempt2)

        // Assert - Accumulate Responses
        finalResponse.failedEntryCount shouldBe 0
        finalResponse.attempts.size shouldBe 2
        finalResponse.entries.size shouldBe 2
        finalResponse.entries[0].eventId shouldBe "id-1"
        finalResponse.entries[1].eventId shouldBe "id-2"
    }

    test("sendCommand should interact with metrics and gracefully throw on failure") {
        // Arrange
        val mockClient = mockk<EventBridgeClient>()
        val mockMetrics = mockk<Metrics>(relaxed = true)
        val eventBridgeClientFactory = mockk<EventBridgeClientFactory>()
        every { eventBridgeClientFactory.createClient(any()) } returns mockClient
        coEvery { mockClient.putEvents(any()) } throws RuntimeException("AWS Network Error")

        val connector = EventBridgeConnector(
            pipelineId = "test-pipeline",
            envConfig = envConfig(),
            clientFactory = eventBridgeClientFactory,
            retryConfig = RetryConfig(),
            timeout = 1000.milliseconds,
            opt = ConnectorOptions(metrics = mockMetrics)
        )

        val request = PutEventsRequest { entries = emptyList() }
        val runtimeException = RuntimeException("AWS Network Error")
        
        coEvery { mockClient.putEvents(any()) } throws runtimeException

        // Act
        val exception = shouldThrow<RuntimeException> {
            connector.sendCommand(request, "mockContext")
        }
        
        // Assert
        exception.message shouldBe "AWS Network Error"
        coVerify(exactly = 1) { 
            mockMetrics.capture(mockClient, request, "eventbridge", any(), "mockContext") 
        }
    }

    test("putEvents should automatically handle retries on partial failure") {
        // Arrange
        val mockClient = mockk<EventBridgeClient>()
        val eventBridgeClientFactory = mockk<EventBridgeClientFactory>()
        every { eventBridgeClientFactory.createClient(any()) } returns mockClient
        val connector = EventBridgeConnector(
            pipelineId = "test-pipeline",
            envConfig = envConfig(),
            clientFactory = eventBridgeClientFactory,
            timeout = 1000.milliseconds, 
            retryConfig = RetryConfig(maxRetries = 2, retryWait = 10L) // Small delay for fast test
        )

        val initialRequest = PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry { source = "source1" },
                PutEventsRequestEntry { source = "source2" }
            )
        }

        val firstResponse = PutEventsResponse {
            failedEntryCount = 1
            entries = listOf(
                PutEventsResultEntry { eventId = "id-1" },
                PutEventsResultEntry { errorCode = "InternalFailure" }
            )
        }

        val secondResponse = PutEventsResponse {
            failedEntryCount = 0
            entries = listOf(
                PutEventsResultEntry { eventId = "id-2" }
            )
        }

        // Setup the client mock to fail a specific entry the first time, then succeed the second time
        coEvery { mockClient.putEvents(any()) } returns  firstResponse andThen secondResponse

        // Act
        val result = connector.putEvents(initialRequest)

        // Assert
        result.failedEntryCount shouldBe 0
        result.attempts.size shouldBe 2
        result.entries.size shouldBe 2
        result.entries[0].eventId shouldBe "id-1"
        result.entries[1].eventId shouldBe "id-2"
    }
})