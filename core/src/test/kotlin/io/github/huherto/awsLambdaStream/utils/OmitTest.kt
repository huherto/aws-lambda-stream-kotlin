package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.JsonEvent
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test

class OmitTest {
    
    @Test
    fun `should omit a single top-level key`() {
        val event = JsonEvent(
            """
            {
              "id": "event-1",
              "type": "ORDER_CREATED",
              "partitionKey": "partition-1",
              "payload": {
                "orderId": "order-1"
              }
            }
            """.trimIndent()
        )

        val result = omit(event, "partitionKey")

        Json.parseToJsonElement(result) shouldBe Json.parseToJsonElement(
            """
            {
              "id": "event-1",
              "type": "ORDER_CREATED",
              "payload": {
                "orderId": "order-1"
              }
            }
            """.trimIndent()
        )
    }

    @Test
    fun `should omit multiple top-level keys`() {
        val event = JsonEvent(
            """
            {
              "id": "event-1",
              "timestamp": 123456789,
              "type": "ORDER_CREATED",
              "tags": {
                "source": "orders"
              },
              "payload": {
                "orderId": "order-1"
              }
            }
            """.trimIndent()
        )

        val result = omit(event, "timestamp", "tags")

        Json.parseToJsonElement(result) shouldBe Json.parseToJsonElement(
            """
            {
              "id": "event-1",
              "type": "ORDER_CREATED",
              "payload": {
                "orderId": "order-1"
              }
            }
            """.trimIndent()
        )
    }

    @Test
    fun `should only omit top-level keys and preserve nested keys with same name`() {
        val event = JsonEvent(
            """
            {
              "id": "event-1",
              "type": "ORDER_CREATED",
              "payload": {
                "id": "order-1",
                "amount": 100
              }
            }
            """.trimIndent()
        )

        val result = omit(event, "id")
        val resultObject = Json.parseToJsonElement(result).jsonObject

        resultObject shouldBe Json.parseToJsonElement(
            """
            {
              "type": "ORDER_CREATED",
              "payload": {
                "id": "order-1",
                "amount": 100
              }
            }
            """.trimIndent()
        ).jsonObject
    }
}
