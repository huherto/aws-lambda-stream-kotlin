package org.myorg.payments

import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.Event
import io.kopipes.core.EventCodec
import io.kopipes.core.EventReference
import io.kopipes.core.RawRecord
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * The accounts subsystem's **external** contract, as it defines it. Not ours: we read it liberally,
 * ignoring fields we don't know, so a change on their side stops here and never reaches the BFF.
 */
@Serializable
data class ExternalAccount(
    val number: String? = null,
    val kind: String? = null,
    /** Their name for our `customerId`. */
    val holderId: String? = null,
    /** The announced rename of `holderId`: during their three deployments either field may come. */
    val ownerId: String? = null,
    val status: String? = null,
    val currency: String? = null,
)

/** `ACCOUNTS.ACCOUNT_OPENED`, `ACCOUNTS.ACCOUNT_UPDATED` or `ACCOUNTS.ACCOUNT_CLOSED`. */
@Serializable
data class AccountsEvent(
    val type: String,
    val account: ExternalAccount? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    /** The event exactly as we heard it, kept for `raw` so the lake keeps what arrived. */
    @Transient val original: JsonElement? = null,
) : Event {

    companion object {
        const val ACCOUNT_OPENED = "ACCOUNTS.ACCOUNT_OPENED"
        const val ACCOUNT_UPDATED = "ACCOUNTS.ACCOUNT_UPDATED"
        const val ACCOUNT_CLOSED = "ACCOUNTS.ACCOUNT_CLOSED"
    }

    override fun eventType(): String = type

    override fun toString(): String = original?.toString() ?: AccountsEventCodec.encode(this)

    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

private val accountsJson = Json { ignoreUnknownKeys = true; explicitNulls = false }

object AccountsEventCodec : EventCodec {
    override fun decode(eventAsString: String): Event {
        val element = accountsJson.parseToJsonElement(eventAsString)
        require(element is JsonObject) { "An ACCOUNTS event must be a JSON object" }
        return accountsJson.decodeFromJsonElement(AccountsEvent.serializer(), element).copy(original = element)
    }

    override fun encode(event: Event): String {
        require(event is AccountsEvent) { "AccountsEventCodec can only encode AccountsEvent, got ${event::class.qualifiedName}" }
        return accountsJson.encodeToString(AccountsEvent.serializer(), event)
    }
}
