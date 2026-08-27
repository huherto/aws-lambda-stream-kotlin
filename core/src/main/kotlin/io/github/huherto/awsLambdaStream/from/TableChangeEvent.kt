package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.EnvelopeEncryptionMetadata
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventReference
import io.github.huherto.awsLambdaStream.RawRecord
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.encodeToString

@Serializable
data class TableChangeEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    val type: String? = null,
) : Event {

    override fun eventType(): String {
        return type ?: "table_change"
    }

    override fun toString(): String {
        return encodeToString(this)
    }

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): Event = copy(
        id = id,
        timestamp = timestamp,
        partitionKey = partitionKey,
        tags = tags,
        raw = raw,
        eem = eem,
        triggers = triggers
    )
}