package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class RetryExecutorTest {

    @Test
    fun `should retry only when failed entry count is greater than zero`() {
        // Arrange
        val strategy = EventBridgeRetryStrategy()

        val successfulResponse = PutEventsResponse {
            failedEntryCount = 0
        }
        val failedResponse = PutEventsResponse {
            failedEntryCount = 1
        }

        // Act
        val shouldRetrySuccessful = strategy.shouldRetry(successfulResponse)
        val shouldRetryFailed = strategy.shouldRetry(failedResponse)

        // Assert
        shouldRetrySuccessful shouldBe false
        shouldRetryFailed shouldBe true
    }

    @Test
    fun `should create next request with only failed entries`() {
        // Arrange
        val strategy = EventBridgeRetryStrategy()

        val originalRequest = PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry { source = "source-1" },
                PutEventsRequestEntry { source = "source-2" },
                PutEventsRequestEntry { source = "source-3" }
            )
        }

        val response = PutEventsResponse {
            entries = listOf(
                PutEventsResultEntry { eventId = "event-1" },
                PutEventsResultEntry { errorCode = "InternalFailure" },
                PutEventsResultEntry { errorCode = "ThrottlingException" }
            )
            failedEntryCount = 2
        }

        // Act
        val nextRequest = strategy.nextRequest(originalRequest, response)

        // Assert
        nextRequest.entries shouldBe listOf(
            PutEventsRequestEntry { source = "source-2" },
            PutEventsRequestEntry { source = "source-3" }
        )
    }

    @Test
    fun `should combine attempts by keeping successful entries and final retry metadata`() {
        // Arrange
        val strategy = EventBridgeRetryStrategy()

        val firstAttempt = PutEventsResponse {
            failedEntryCount = 1
            entries = listOf(
                PutEventsResultEntry { eventId = "event-1" },
                PutEventsResultEntry { errorCode = "InternalFailure" }
            )
        }

        val secondAttempt = PutEventsResponse {
            failedEntryCount = 0
            entries = listOf(
                PutEventsResultEntry { eventId = "event-2" }
            )
        }

        // Act
        val result = strategy.combineAttempts(listOf(firstAttempt), secondAttempt)

        // Assert
        result.failedEntryCount shouldBe 0
        result.attempts shouldBe listOf(firstAttempt, secondAttempt)
        result.entries.mapNotNull { it.eventId } shouldBe listOf("event-1", "event-2")
    }

    @Test
    fun `should execute once when first response does not require retry`() = runTest {
        // Arrange
        val strategy = mockk<RetryStrategy<String, String, String>>()
        val retryConfig = RetryConfig(maxRetries = 3, retryWait = 100.milliseconds)
        val sendCalls = mutableListOf<String>()

        val executor = RetryExecutor(
            retryConfig = retryConfig,
            strategy = strategy,
            send = { request ->
                sendCalls += request
                "final-response"
            }
        )

        io.mockk.every { strategy.shouldRetry("final-response") } returns false
        io.mockk.every { strategy.combineAttempts(emptyList(), "final-response") } returns "combined"

        // Act
        val result = executor.execute("initial-request")

        // Assert
        result shouldBe "combined"
        sendCalls shouldBe listOf("initial-request")
        io.mockk.verify(exactly = 1) { strategy.shouldRetry("final-response") }
        io.mockk.verify(exactly = 1) { strategy.combineAttempts(emptyList(), "final-response") }
        io.mockk.verify(exactly = 0) { strategy.nextRequest(any(), any()) }
    }

    @Test
    fun `should retry with exponential delay and stop after max retries`() = runTest {
        // Arrange
        val strategy = mockk<RetryStrategy<String, String, String>>()
        val retryConfig = RetryConfig(maxRetries = 2, retryWait = 100.milliseconds)
        val sendCalls = mutableListOf<String>()

        val executor = RetryExecutor(
            retryConfig = retryConfig,
            strategy = strategy,
            send = { request ->
                sendCalls += request
                "retry-response"
            }
        )

        io.mockk.every { strategy.shouldRetry("retry-response") } returns true
        io.mockk.every { strategy.nextRequest(any(), "retry-response") } returns "next-request"

        // Act
        val exception = shouldThrow<IllegalStateException> {
            executor.execute("initial-request")
        }

        // Assert
        exception.message shouldBe "Maximum retry attempts exceeded."
        sendCalls shouldBe listOf("initial-request", "next-request", "next-request")
        io.mockk.verify(exactly = 3) { strategy.shouldRetry("retry-response") }
        io.mockk.verify(exactly = 3) { strategy.nextRequest(any(), "retry-response") }
    }
}