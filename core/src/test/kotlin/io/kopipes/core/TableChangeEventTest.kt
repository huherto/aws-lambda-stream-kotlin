package io.kopipes.core

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test

class TableChangeEventTest {

    @Serializable
    data class SamplePayload(val name: String, val count: Int)

    class DummyTableChangeEvent(
        override val id: String? = "evt-123",
        override val timestamp: Long? = 1700000000L,
        override val partitionKey: String? = "pk-1",
        override val tags: Map<String, String>? = mapOf("env" to "test"),
        override val raw: RawRecord? = null,
        override val eem: EnvelopeEncryptionMetadata? = null,
        override val triggers: List<EventReference>? = null,
        override val type: String? = null,
        private val dataMap: Map<String, Any?> = emptyMap(),
    ) : TableChangeEvent() {
        override fun getPk(): String? = dataMap["pk"] as? String
        override fun getTtl(): String? = dataMap["ttl"] as? String
        override fun getData(): String? = dataMap["data"] as? String
        override fun getEvent(): String? = dataMap["event"] as? String
        override fun getDiscriminator(): String? = dataMap["discriminator"] as? String
        override fun getSuffix(): String? = dataMap["suffix"] as? String
        override fun isDeleted(): Boolean = dataMap["deleted"] == true
        override fun latched(): Boolean = dataMap["latched"] == true
        override fun getS(fieldName: String): String? = dataMap[fieldName] as? String
        override fun getDouble(fieldName: String): Double? = (dataMap[fieldName] as? Number)?.toDouble()
        override fun getLong(fieldName: String): Long? = (dataMap[fieldName] as? Number)?.toLong()

        override fun copyEvent(
            id: String?,
            timestamp: Long?,
            partitionKey: String?,
            tags: Map<String, String>?,
            raw: RawRecord?,
            eem: EnvelopeEncryptionMetadata?,
            triggers: List<EventReference>?
        ): Event = DummyTableChangeEvent(
            id = id,
            timestamp = timestamp,
            partitionKey = partitionKey,
            tags = tags,
            raw = raw,
            eem = eem,
            triggers = triggers,
            type = type,
            dataMap = dataMap
        )
    }

    @Test
    fun `default eventType should return table_change when type is null`() {
        val event = DummyTableChangeEvent(type = null)
        event.eventType() shouldBe "table_change"
    }

    @Test
    fun `eventType should return type when type is specified`() {
        val event = DummyTableChangeEvent(type = "custom_change")
        event.eventType() shouldBe "custom_change"
    }

    @Test
    fun `getDecodedObject should deserialize json string correctly`() {
        val event = DummyTableChangeEvent(
            dataMap = mapOf("payload" to """{"name":"test-item","count":42}""")
        )

        val payload = event.getDecodedObject<SamplePayload>("payload")
        payload?.name shouldBe "test-item"
        payload?.count shouldBe 42
    }

    @Test
    fun `getDecodedObject should return null when field is missing`() {
        val event = DummyTableChangeEvent(dataMap = emptyMap())
        val payload = event.getDecodedObject<SamplePayload>("non_existent")
        payload.shouldBeNull()
    }
}
