package io.github.huherto.awsLambdaStream

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test

class JsonEventTest {

    @Test
    fun `getJsonElementByPath returns nested elements and null for invalid paths`() {
        // Arrange
        val jsonObject = Json.parseToJsonElement(
            """
            {
              "customer": {
                "address": {
                  "city": "New York",
                  "zip": "10001"
                },
                "active": true
              }
            }
            """.trimIndent()
        ).jsonObject

        // Act
        val nestedObject = getJsonElementByPath(jsonObject, "customer.address")
        val nestedPrimitive = getJsonElementByPath(jsonObject, "customer.address.city")
        val missingElement = getJsonElementByPath(jsonObject, "customer.profile.name")
        val pathThroughPrimitive = getJsonElementByPath(jsonObject, "customer.active.value")

        // Assert
        nestedObject shouldBe jsonObject["customer"]?.jsonObject?.get("address")
        nestedPrimitive shouldBe JsonPrimitive("New York")
        missingElement.shouldBeNull()
        pathThroughPrimitive.shouldBeNull()
    }

    @Test
    fun `getJsonObjectByPath and getJsonPrimitiveByPath return values only when type matches`() {
        // Arrange
        val jsonObject = Json.parseToJsonElement(
            """
            {
              "customer": {
                "address": {
                  "city": "New York"
                }
              }
            }
            """.trimIndent()
        ).jsonObject

        // Act
        val objectResult = getJsonObjectByPath(jsonObject, "customer.address")
        val objectForPrimitivePath = getJsonObjectByPath(jsonObject, "customer.address.city")
        val primitiveResult = getJsonPrimitiveByPath(jsonObject, "customer.address.city")
        val primitiveForObjectPath = getJsonPrimitiveByPath(jsonObject, "customer.address")

        // Assert
        objectResult shouldBe jsonObject["customer"]?.jsonObject?.get("address")
        objectForPrimitivePath.shouldBeNull()
        primitiveResult shouldBe JsonPrimitive("New York")
        primitiveForObjectPath.shouldBeNull()
    }

    @Test
    fun `JsonEvent exposes event fields from json`() {
        // Arrange
        val jsonString = """
            {
              "id": "event-1",
              "timestamp": 123456789,
              "partitionKey": "partition-1",
              "type": "ORDER_CREATED",
              "tags": {
                "source": "orders",
                "priority": "high"
              },
              "eem": {
                "keyId": "key-1",
                "algorithm": "AES"
              },
              "triggers": [
                "trigger-1",
                "trigger-2"
              ],
              "payload": {
                "orderId": "order-1"
              }
            }
        """.trimIndent()

        // Act
        val event = JsonEvent(jsonString)

        // Assert
        event.id shouldBe "event-1"
        event.timestamp shouldBe 123456789L
        event.partitionKey shouldBe "partition-1"
        event.eventType() shouldBe "ORDER_CREATED"
        event.tags?.shouldContainExactly(mapOf(
            "source" to "orders",
            "priority" to "high",
        ))
        event.eem shouldBe mapOf(
            "keyId" to "key-1",
            "algorithm" to "AES",
        )
        event.triggers shouldContainExactly listOf(
            EventReference("trigger-1"),
            EventReference("trigger-2"),
        )
        event.raw shouldBe null
    }

    @Test
    fun `JsonEvent returns unknown type and null optional fields when properties are absent`() {
        // Arrange
        val jsonString = """{"payload":{"value":"test"}}"""

        // Act
        val event = JsonEvent(jsonString)

        // Assert
        event.id.shouldBeNull()
        event.timestamp.shouldBeNull()
        event.partitionKey.shouldBeNull()
        event.tags.shouldBeNull()
        event.eem.shouldBeNull()
        event.triggers.shouldBeNull()
        event.eventType() shouldBe "unknown"
    }

    @Test
    fun `JsonEvent path helpers return nested object and primitive values`() {
        // Arrange
        val event = JsonEvent(
            """
            {
              "payload": {
                "customer": {
                  "id": "customer-1",
                  "name": "Jane Doe"
                }
              }
            }
            """.trimIndent()
        )

        // Act
        val customer = event.jsonObject("payload.customer")
        val customerId = event.jsonPrimitive("payload.customer.id")
        val missingValue = event.jsonPrimitive("payload.customer.email")
        val objectAsPrimitive = event.jsonPrimitive("payload.customer")
        val primitiveAsObject = event.jsonObject("payload.customer.id")

        // Assert
        customer shouldBe JsonObject(
            mapOf(
                "id" to JsonPrimitive("customer-1"),
                "name" to JsonPrimitive("Jane Doe"),
            )
        )
        customerId shouldBe JsonPrimitive("customer-1")
        missingValue.shouldBeNull()
        objectAsPrimitive.shouldBeNull()
        primitiveAsObject.shouldBeNull()
    }

    @Test
    fun `encoded returns canonical json representation`() {
        // Arrange
        val jsonString = """
            {
              "id": "event-1",
              "type": "ORDER_CREATED",
              "payload": {
                "orderId": "order-1"
              }
            }
        """.trimIndent()
        val event = JsonEvent(jsonString)

        // Act
        val encoded = event.encoded()

        // Assert
        Json.parseToJsonElement(encoded) shouldBe Json.parseToJsonElement(jsonString)
    }

    @Test
    fun `setters do not mutate JsonEvent values`() {
        // Arrange
        val event = JsonEvent(
            """
            {
              "id": "event-1",
              "timestamp": 123,
              "partitionKey": "partition-1",
              "tags": {
                "source": "orders"
              },
              "eem": {
                "keyId": "key-1"
              },
              "triggers": [
                "trigger-1"
              ]
            }
            """.trimIndent()
        )

        // Act
        event.id = "event-2"
        event.timestamp = 456
        event.partitionKey = "partition-2"
        event.tags = mapOf("source" to "payments")
        event.raw = JsonObject(emptyMap())
        event.eem = mapOf("keyId" to "key-2")
        event.triggers = listOf(EventReference("trigger-2"))

        // Assert
        event.id shouldBe "event-1"
        event.timestamp shouldBe 123L
        event.partitionKey shouldBe "partition-1"
        event.tags?.shouldContainExactly(mapOf("source" to "orders"))
        event.raw shouldBe null
        event.eem shouldBe mapOf("keyId" to "key-1")
        event.triggers shouldContainExactly listOf(EventReference("trigger-1"))
    }
}
