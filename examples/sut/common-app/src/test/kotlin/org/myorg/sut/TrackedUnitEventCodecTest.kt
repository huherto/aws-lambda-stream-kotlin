package org.myorg.sut

import io.github.huherto.awsLambdaStream.EventReference
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.test.Test

class TrackedUnitEventCodecTest {

    @Test
    fun `should round trip encode and decode a tracked unit event`() {
        val codec = TrackedUnitEventCodec

        val original = ShipmentPickedUpEvent(
            carrierName = "UPS",
            id = "event-1",
            timestamp = 123456789L,
            partitionKey = "unit-1",
            tags = mapOf("source" to "test"),
            entity = TrackedUnit().apply {
                id = "unit-1"
                senderFullName = "Alice Sender"
                trackingNumber = "TRACK-123"
            },
            location = "Berlin",
            result = "picked up",
            triggers = listOf(
                EventReference(id = "trigger-1", type = "trigger-type-1", timestamp = 111L),
                EventReference(id = "trigger-2", type = "trigger-type-2", timestamp = 222L)
            )
        )

        val encoded = codec.encode(original)
        val decoded = codec.decode(encoded)

        decoded.shouldBeInstanceOf<ShipmentPickedUpEvent>()
        decoded.id shouldBe original.id
        decoded.timestamp shouldBe original.timestamp
        decoded.partitionKey shouldBe original.partitionKey
        decoded.tags shouldBe original.tags
        decoded.location shouldBe original.location
        decoded.result shouldBe original.result
        decoded.entity?.id shouldBe original.entity?.id
        decoded.entity?.senderFullName shouldBe original.entity?.senderFullName
        decoded.entity?.trackingNumber shouldBe original.entity?.trackingNumber
        decoded.triggers shouldBe original.triggers
    }

    @Test
    fun `should round trip encode and decode a verify target address event`() {
        val codec = TrackedUnitEventCodec

        val original = VerifyTargetAddressEvent(
            id = "event-2",
            timestamp = 987654321L,
            partitionKey = "unit-2",
            tags = mapOf("source" to "validation"),
            location = "Warsaw",
            result = "verified",
            entity = TrackedUnit().apply {
                id = "unit-2"
                senderFullName = "Bob Sender"
                trackingNumber = "TRACK-456"
            }
        )

        val encoded = codec.encode(original)
        val decoded = codec.decode(encoded)

        decoded.shouldBeInstanceOf<VerifyTargetAddressEvent>()
        decoded.id shouldBe original.id
        decoded.timestamp shouldBe original.timestamp
        decoded.partitionKey shouldBe original.partitionKey
        decoded.tags shouldBe original.tags
        decoded.location shouldBe original.location
        decoded.result shouldBe original.result
        decoded.entity?.id shouldBe original.entity?.id
        decoded.entity?.senderFullName shouldBe original.entity?.senderFullName
        decoded.entity?.trackingNumber shouldBe original.entity?.trackingNumber
    }

}