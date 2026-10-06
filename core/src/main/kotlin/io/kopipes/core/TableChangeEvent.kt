package io.kopipes.core

import kotlinx.serialization.Serializable

/**
 * Base abstract class for table change stream events.
 */
@Serializable
abstract class TableChangeEvent : Event {
    override val id: String? get() = null
    override val timestamp: Long? get() = null
    override val partitionKey: String? get() = null
    override val tags: Map<String, String>? get() = null
    override val raw: RawRecord? get() = null
    override val eem: EnvelopeEncryptionMetadata? get() = null
    override val triggers: List<EventReference>? get() = null
    open val type: String? get() = null

    override fun eventType(): String {
        return type ?: "table_change"
    }

    abstract override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): TableChangeEvent
}
