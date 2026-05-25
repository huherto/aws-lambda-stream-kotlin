package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemResponse
import aws.sdk.kotlin.services.dynamodb.model.KeysAndAttributes
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class DynamoDbBatchGetRetryStrategyTest {

    private val strategy = DynamoDbBatchGetRetryStrategy()

    @Test
    fun `shouldRetry returns true only when response has unprocessed keys`() {
        // Arrange
        val responseWithMissingUnprocessedKeys = BatchGetItemResponse { }
        val responseWithEmptyUnprocessedKeys = BatchGetItemResponse {
            unprocessedKeys = emptyMap()
        }
        val responseWithUnprocessedKeys = BatchGetItemResponse {
            unprocessedKeys = mapOf(
                "events" to KeysAndAttributes {
                    keys = listOf(mapOf("pk" to AttributeValue.S("event-1")))
                }
            )
        }

        // Act
        val shouldRetryMissingUnprocessedKeys = strategy.shouldRetry(responseWithMissingUnprocessedKeys)
        val shouldRetryEmptyUnprocessedKeys = strategy.shouldRetry(responseWithEmptyUnprocessedKeys)
        val shouldRetryUnprocessedKeys = strategy.shouldRetry(responseWithUnprocessedKeys)

        // Assert
        shouldRetryMissingUnprocessedKeys shouldBe false
        shouldRetryEmptyUnprocessedKeys shouldBe false
        shouldRetryUnprocessedKeys shouldBe true
    }

    @Test
    fun `combineAttempts combines items for same table in attempt order and keeps final response metadata`() {
        // Arrange
        val firstItem = mapOf("pk" to AttributeValue.S("event-1"))
        val secondItem = mapOf("pk" to AttributeValue.S("event-2"))
        val finalItem = mapOf("pk" to AttributeValue.S("event-3"))
        val finalUnprocessedKeys = mapOf(
            "events" to KeysAndAttributes {
                keys = listOf(mapOf("pk" to AttributeValue.S("event-4")))
            }
        )

        val firstAttempt = BatchGetItemResponse {
            responses = mapOf("events" to listOf(firstItem))
        }
        val secondAttempt = BatchGetItemResponse {
            responses = mapOf("events" to listOf(secondItem))
        }
        val finalResponse = BatchGetItemResponse {
            responses = mapOf("events" to listOf(finalItem))
            unprocessedKeys = finalUnprocessedKeys
        }

        // Act
        val result = strategy.combineAttempts(listOf(firstAttempt, secondAttempt), finalResponse)

        // Assert
        result.responses?.shouldContainExactly(
            mapOf(
                "events" to listOf(firstItem, secondItem, finalItem)
            )
        )
        result.unprocessedKeys shouldBe finalUnprocessedKeys
    }

    @Test
    fun `combineAttempts combines multiple tables independently and ignores missing responses`() {
        // Arrange
        val firstEvent = mapOf("pk" to AttributeValue.S("event-1"))
        val firstUser = mapOf("pk" to AttributeValue.S("user-1"))
        val secondEvent = mapOf("pk" to AttributeValue.S("event-2"))
        val finalUser = mapOf("pk" to AttributeValue.S("user-2"))

        val firstAttempt = BatchGetItemResponse {
            responses = mapOf(
                "events" to listOf(firstEvent),
                "users" to listOf(firstUser)
            )
        }
        val secondAttemptWithoutResponses = BatchGetItemResponse { }
        val finalResponse = BatchGetItemResponse {
            responses = mapOf(
                "events" to listOf(secondEvent),
                "users" to listOf(finalUser)
            )
            unprocessedKeys = emptyMap()
        }

        // Act
        val result = strategy.combineAttempts(
            attempts = listOf(firstAttempt, secondAttemptWithoutResponses),
            finalResponse = finalResponse,
        )

        // Assert
        result.responses?.keys?.shouldContainExactly(setOf("events", "users"))
        result.responses?.get("events") shouldBe listOf(firstEvent, secondEvent)
        result.responses?.get("users") shouldBe listOf(firstUser, finalUser)
        result.unprocessedKeys?.shouldContainExactly(emptyMap())
    }
}
