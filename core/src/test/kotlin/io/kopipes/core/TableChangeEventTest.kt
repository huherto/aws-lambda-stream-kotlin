package io.kopipes.core

import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test

class TableChangeEventTest {

    @Serializable
    private data class ConcreteTableChangeEvent(
        override val id: String? = null,
        override val timestamp: Long? = null,
        override val partitionKey: String? = null,
        override val tags: Map<String, String>? = null,
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        override val type: String? = null,
    ) : TableChangeEvent() {
        override fun copyEvent(
            id: String?,
            timestamp: Long?,
            partitionKey: String?,
            tags: Map<String, String>?,
            raw: RawRecord?,
            eem: EnvelopeEncryptionMetadata?,
            triggers: List<EventReference>?
        ): ConcreteTableChangeEvent = copy(
            id = id,
            timestamp = timestamp,
            partitionKey = partitionKey,
            tags = tags,
            raw = raw,
            eem = eem,
            triggers = triggers
        )
    }

    @Test
    fun `eventType should default to table_change when type is null`() {
        val event = ConcreteTableChangeEvent(id = "evt-1")
        event.eventType() shouldBe "table_change"
    }

    @Test
    fun `eventType should return type when specified`() {
        val event = ConcreteTableChangeEvent(id = "evt-1", type = "custom_change")
        event.eventType() shouldBe "custom_change"
    }

    @Test
    fun `copyEvent should produce updated change event`() {
        val original = ConcreteTableChangeEvent(id = "evt-1", timestamp = 1000L)
        val copy = original.copyEvent(id = "evt-2", tags = mapOf("k" to "v"))
        copy.id shouldBe "evt-2"
        copy.timestamp shouldBe 1000L
        copy.tags shouldBe mapOf("k" to "v")
    }
}
