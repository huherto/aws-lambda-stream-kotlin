package io.kopipes.aws.from

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAV

class RecordImageTest {

    @Serializable
    data class SamplePayload(val name: String, val count: Int)

    @Test
    fun `should read all standard properties and values via helper`() {
        // Arrange
        val map = mapOf(
            "pk" to StreamAV("PK#123"),
            "ttl" to StreamAV().withN("1700000000"),
            "data" to StreamAV("sample-data"),
            "event" to StreamAV("""{"name":"test-event","count":5}"""),
            "discriminator" to StreamAV("DISC_A"),
            "suffix" to StreamAV("v2"),
            "deleted" to StreamAV().withBOOL(true),
            "latched" to StreamAV().withBOOL(false),
            "score" to StreamAV().withN("42.75"),
            "quantity" to StreamAV().withN("100"),
            "bigNumber" to StreamAV().withN("9876543210"),
            "isActive" to StreamAV().withBOOL(true),
            "nullableField" to StreamAV().withNULL(true),
        )
        val image = RecordImage(map)

        // Act & Assert
        image.getPk() shouldBe "PK#123"
        image.getTtl() shouldBe "1700000000"
        image.getData() shouldBe "sample-data"
        image.getEvent() shouldBe """{"name":"test-event","count":5}"""
        image.getDiscriminator() shouldBe "DISC_A"
        image.getSuffix() shouldBe "v2"
        image.isDeleted() shouldBe true
        image.latched() shouldBe false

        image.getS("pk") shouldBe "PK#123"
        image.getDouble("score") shouldBe 42.75
        image.getInt("quantity") shouldBe 100
        image.getLong("bigNumber") shouldBe 9876543210L
        image.getBool("isActive") shouldBe true
        image.isNull("nullableField") shouldBe true

        val decoded = image.getDecodedObject<SamplePayload>("event")
        decoded shouldNotBe null
        decoded?.name shouldBe "test-event"
        decoded?.count shouldBe 5
    }

    @Test
    fun `should return null or false for missing attributes`() {
        // Arrange
        val image = RecordImage(emptyMap())

        // Act & Assert
        image.getPk().shouldBeNull()
        image.getTtl().shouldBeNull()
        image.getData().shouldBeNull()
        image.getEvent().shouldBeNull()
        image.getDiscriminator().shouldBeNull()
        image.getSuffix().shouldBeNull()
        image.isDeleted() shouldBe false
        image.latched() shouldBe false

        image.getS("missing").shouldBeNull()
        image.getDouble("missing").shouldBeNull()
        image.getInt("missing").shouldBeNull()
        image.getLong("missing").shouldBeNull()
        image.getBool("missing").shouldBeNull()
        image.isNull("missing").shouldBeNull()
        image.getDecodedObject<SamplePayload>("missing").shouldBeNull()
    }

    @Test
    fun `should support Map delegation, equals, hashCode and toString`() {
        // Arrange
        val map1 = mapOf("pk" to StreamAV("PK#1"))
        val map2 = mapOf("pk" to StreamAV("PK#1"))
        val map3 = mapOf("pk" to StreamAV("PK#2"))

        val image1 = RecordImage(map1)
        val image2 = RecordImage(map2)
        val image3 = RecordImage(map3)

        // Act & Assert
        image1.size shouldBe 1
        image1["pk"] shouldBe StreamAV("PK#1")
        image1.containsKey("pk") shouldBe true

        image1 shouldBe image2
        image1.hashCode() shouldBe image2.hashCode()
        image1 shouldNotBe image3
        image1.toString() shouldBe map1.toString()
    }
}
