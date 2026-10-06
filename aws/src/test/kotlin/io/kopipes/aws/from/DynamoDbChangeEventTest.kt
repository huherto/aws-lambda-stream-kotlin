package io.kopipes.aws.from

import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.EventReference
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAV

class DynamoDbChangeEventTest {

    @Serializable
    data class OrderPayload(val orderId: String, val amount: Double)

    @Test
    fun `getters should correctly delegate to RecordPair new image`() {
        // Arrange
        val newImage = RecordImage(
            mapOf(
                "pk" to StreamAV("PK#100"),
                "ttl" to StreamAV().withN("1700000000"),
                "data" to StreamAV("data-value"),
                "event" to StreamAV("""{"orderId":"order-123","amount":99.99}"""),
                "discriminator" to StreamAV("CORREL"),
                "suffix" to StreamAV("v1"),
                "deleted" to StreamAV().withBOOL(true),
                "latched" to StreamAV().withBOOL(true),
                "customStr" to StreamAV("custom-val"),
                "customDouble" to StreamAV().withN("42.5"),
                "customLong" to StreamAV().withN("9876543210"),
            )
        )
        val oldImage = RecordImage(
            mapOf(
                "pk" to StreamAV("PK#100-old"),
            )
        )
        val recordPair = RecordPair(new = newImage, old = oldImage)
        val event = DynamoDbChangeEvent(
            id = "event-id-1",
            timestamp = 1700000000000L,
            partitionKey = "PK#100",
            tags = mapOf("tag1" to "val1"),
            eem = EnvelopeEncryptionMetadata(masterKeyAlias = "alias/my-key"),
            triggers = listOf(EventReference(id = "trig-1")),
            type = "ORDER_CHANGED",
            raw = recordPair,
        )

        // Act & Assert
        event.id shouldBe "event-id-1"
        event.timestamp shouldBe 1700000000000L
        event.partitionKey shouldBe "PK#100"
        event.eventType() shouldBe "ORDER_CHANGED"
        event.recordPair shouldBe recordPair
        event.newImage shouldBe newImage
        event.oldImage shouldBe oldImage

        event.getPk() shouldBe "PK#100"
        event.getTtl() shouldBe "1700000000"
        event.getData() shouldBe "data-value"
        event.getEvent() shouldBe """{"orderId":"order-123","amount":99.99}"""
        event.getDiscriminator() shouldBe "CORREL"
        event.getSuffix() shouldBe "v1"
        event.isDeleted() shouldBe true
        event.latched() shouldBe true
        event.getS("customStr") shouldBe "custom-val"
        event.getDouble("customDouble") shouldBe 42.5
        event.getLong("customLong") shouldBe 9876543210L

        val order = event.getDecodedObject<OrderPayload>("event")
        order?.orderId shouldBe "order-123"
        order?.amount shouldBe 99.99
    }

    @Test
    fun `getters should return null or false when new image or fields are absent`() {
        // Arrange
        val event = DynamoDbChangeEvent(
            id = "empty-event",
            raw = null
        )

        // Act & Assert
        event.getPk().shouldBeNull()
        event.getTtl().shouldBeNull()
        event.getData().shouldBeNull()
        event.getEvent().shouldBeNull()
        event.getDiscriminator().shouldBeNull()
        event.getSuffix().shouldBeNull()
        event.isDeleted() shouldBe false
        event.latched() shouldBe false
        event.getS("custom").shouldBeNull()
        event.getDouble("custom").shouldBeNull()
        event.getLong("custom").shouldBeNull()
        event.eventType() shouldBe "table_change"
    }

    @Test
    fun `copyEvent should create an updated instance with new metadata`() {
        // Arrange
        val original = DynamoDbChangeEvent(
            id = "orig-id",
            timestamp = 1000L,
            partitionKey = "orig-pk",
            tags = mapOf("k" to "v"),
            type = "ORIGINAL_TYPE"
        )

        // Act
        val copied = original.copyEvent(
            id = "new-id",
            timestamp = 2000L,
            partitionKey = "new-pk",
            tags = mapOf("k2" to "v2")
        )

        // Assert
        copied.id shouldBe "new-id"
        copied.timestamp shouldBe 2000L
        copied.partitionKey shouldBe "new-pk"
        copied.tags shouldBe mapOf("k2" to "v2")
        copied.type shouldBe "ORIGINAL_TYPE"
    }

    @Test
    fun `toString should produce valid JSON serializable representation`() {
        // Arrange
        val event = DynamoDbChangeEvent(
            id = "evt-serialization",
            timestamp = 1700000000L,
            partitionKey = "pk-test",
            type = "SERIALIZE_TEST"
        )

        // Act
        val jsonStr = event.toString()
        val decoded = Json.decodeFromString<DynamoDbChangeEvent>(jsonStr)

        // Assert
        decoded.id shouldBe "evt-serialization"
        decoded.timestamp shouldBe 1700000000L
        decoded.partitionKey shouldBe "pk-test"
        decoded.type shouldBe "SERIALIZE_TEST"
    }
}
