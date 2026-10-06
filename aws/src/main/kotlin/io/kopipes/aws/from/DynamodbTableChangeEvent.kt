package io.kopipes.aws.from

import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.EventReference
import io.kopipes.core.RawRecord
import io.kopipes.core.TableChangeEvent
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class DynamodbTableChangeEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val type: String? = null,
) : TableChangeEvent() {

    override fun toString(): String {
        return Json.encodeToString(serializer(), this)
    }

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): DynamodbTableChangeEvent = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}

typealias DynamoDbTableChangeEvent = DynamodbTableChangeEvent
