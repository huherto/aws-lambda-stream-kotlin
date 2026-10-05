package io.kopipes.core.resilience

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class RetryExecutorTest {

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

        every { strategy.shouldRetry("final-response") } returns false
        every { strategy.combineAttempts(emptyList(), "final-response") } returns "combined"

        // Act
        val result = executor.execute("initial-request")

        // Assert
        result shouldBe "combined"
        sendCalls shouldBe listOf("initial-request")
        verify(exactly = 1) { strategy.shouldRetry("final-response") }
        verify(exactly = 1) { strategy.combineAttempts(emptyList(), "final-response") }
        verify(exactly = 0) { strategy.nextRequest(any(), any()) }
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

        every { strategy.shouldRetry("retry-response") } returns true
        every { strategy.nextRequest(any(), "retry-response") } returns "next-request"

        // Act
        val exception = shouldThrow<IllegalStateException> {
            executor.execute("initial-request")
        }

        // Assert
        exception.message shouldBe "Maximum retry attempts exceeded."
        sendCalls shouldBe listOf("initial-request", "next-request", "next-request")
        verify(exactly = 3) { strategy.shouldRetry("retry-response") }
        verify(exactly = 3) { strategy.nextRequest(any(), "retry-response") }
    }
}
