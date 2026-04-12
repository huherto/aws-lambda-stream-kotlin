package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class EventBridgeRetryStrategyTest {

    private val strategy = EventBridgeRetryStrategy()

    @Test
    fun `shouldRetry returns true only when there are failed entries`() {
        // Arrange
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
    fun `nextRequest keeps only failed entries and ignores successful entries and missing response items`() {
        // Arrange
        val originalRequest = PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry { source = "source-1" },
                PutEventsRequestEntry { source = "source-2" },
                PutEventsRequestEntry { source = "source-3" },
                PutEventsRequestEntry { source = "source-4" }
            )
        }

        val response = PutEventsResponse {
            entries = listOf(
                PutEventsResultEntry { eventId = "event-1" },
                PutEventsResultEntry { eventId = "event-2" },
                PutEventsResultEntry { errorCode = "InternalFailure" },
                PutEventsResultEntry { errorCode = "ThrottlingException" }
            )
            failedEntryCount = 2
        }

        // Act
        val nextRequest = strategy.nextRequest(originalRequest, response)

        // Assert
        nextRequest.entries shouldBe listOf(
            PutEventsRequestEntry { source = "source-3" },
            PutEventsRequestEntry { source = "source-4" }
        )
    }

    @Test
    fun `combineAttempts returns all successful entries from every attempt and final retry metadata`() {
        // Arrange
        val firstAttempt = PutEventsResponse {
            failedEntryCount = 1
            entries = listOf(
                PutEventsResultEntry { eventId = "event-1" },
                PutEventsResultEntry { errorCode = "InternalFailure" }
            )
        }

        val secondAttempt = PutEventsResponse {
            failedEntryCount = 2
            entries = listOf(
                PutEventsResultEntry { eventId = "event-2" },
                PutEventsResultEntry { errorCode = "ThrottlingException" }
            )
        }

        val finalResponse = PutEventsResponse {
            failedEntryCount = 0
            entries = listOf(
                PutEventsResultEntry { eventId = "event-3" }
            )
        }

        // Act
        val result = strategy.combineAttempts(listOf(firstAttempt, secondAttempt), finalResponse)

        // Assert
        result.failedEntryCount shouldBe 0
        result.attempts shouldBe listOf(firstAttempt, secondAttempt, finalResponse)
        result.entries.mapNotNull { it.eventId } shouldBe listOf("event-1", "event-2", "event-3")
    }
}
