package io.github.huherto.awsLambdaStream.serialization

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class AttributeValueCanonicalJsonTest {

    @Test
    fun `should round trip every attribute value type`() {
        val original = mapOf<String, AttributeValue?>(
            "s" to AttributeValue().withS("text"),
            "n" to AttributeValue().withN("123.456"),
            "bool" to AttributeValue().withBOOL(true),
            "nul" to AttributeValue().withNULL(true),
            "ss" to AttributeValue().withSS("a", "b"),
            "ns" to AttributeValue().withNS("1", "2"),
            "b" to AttributeValue().withB(bytes("binary")),
            "bs" to AttributeValue().withBS(bytes("one"), bytes("two")),
            "l" to AttributeValue().withL(
                AttributeValue().withS("nested"),
                AttributeValue().withN("7"),
            ),
            "m" to AttributeValue().withM(mapOf("inner" to AttributeValue().withS("value"))),
        )

        val decoded = original.toCanonicalJsonObject().toCanonicalAttributeValueMap()

        decoded shouldBe original
    }

    @Test
    fun `should keep sets distinct from lists`() {
        val original = mapOf<String, AttributeValue?>("ss" to AttributeValue().withSS("a", "b"))

        val json = original.toCanonicalJsonObject()
        json.toString() shouldBe """{"ss":{"SS":["a","b"]}}"""

        val decoded = json.toCanonicalAttributeValueMap()
        decoded["ss"]?.ss shouldBe listOf("a", "b")
        decoded["ss"]?.l shouldBe null
    }

    @Test
    fun `should preserve numeric precision beyond what a double can hold`() {
        val big = "123456789012345678901234567890.000000000000001"

        val decoded = mapOf<String, AttributeValue?>("n" to AttributeValue().withN(big))
            .toCanonicalJsonObject()
            .toCanonicalAttributeValueMap()

        decoded["n"]?.n shouldBe big
    }

    @Test
    fun `should carry a null attribute value through as NULL`() {
        val json = mapOf<String, AttributeValue?>("missing" to null).toCanonicalJsonObject()

        json.toString() shouldBe """{"missing":{"NULL":true}}"""
        json.toCanonicalAttributeValueMap()["missing"]?.isNULL shouldBe true
    }

    @Test
    fun `should reject an unknown type tag rather than guess`() {
        val json = Json.parseToJsonElement("""{"XX":"?"}""").jsonObject

        shouldThrow<SerializationException> { json.toCanonicalAttributeValue() }
    }

    @Test
    fun `should encode binary from a buffer that has already been read`() {
        val buffer = bytes("payload").apply { position(limit()) }.duplicate().apply { position(0) }

        val decoded = mapOf<String, AttributeValue?>("b" to AttributeValue().withB(buffer))
            .toCanonicalJsonObject()
            .toCanonicalAttributeValueMap()

        decoded["b"]?.b?.utf8() shouldBe "payload"
        // The source buffer is left where it was, not consumed.
        buffer.position() shouldBe 0
    }

    private fun bytes(text: String): ByteBuffer =
        ByteBuffer.wrap(text.toByteArray(StandardCharsets.UTF_8))

    private fun ByteBuffer.utf8(): String {
        val copy = duplicate()
        val out = ByteArray(copy.remaining())
        copy.get(out)
        return out.toString(StandardCharsets.UTF_8)
    }
}
