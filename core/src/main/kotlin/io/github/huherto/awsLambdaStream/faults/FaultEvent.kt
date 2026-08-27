package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.serialization.snapshots.ErrorSnapshot
import io.github.huherto.awsLambdaStream.serialization.snapshots.UnitOfWorkSnapshot
import kotlinx.serialization.Contextual
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class FaultEvent(
    override val id: String? = null,
    @OptIn(ExperimentalSerializationApi::class)
    @EncodeDefault
    val type: String = FAULT_EVENT_TYPE,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,

    @Contextual
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,

    val err: ErrorSnapshot? = null,
    val uow: UnitOfWorkSnapshot? = null,
    @kotlinx.serialization.Transient
    val runtimeUow: io.github.huherto.awsLambdaStream.UnitOfWork? = null,
    @kotlinx.serialization.Transient
    val faultException: io.github.huherto.awsLambdaStream.FaultException? = null,
) : Event {

    override fun toString(): String {
        return FaultEventCodec.encode(this)
    }

    override fun eventType(): String {
        return FAULT_EVENT_TYPE
    }

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: Any?,
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

private val faultEventJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

object FaultEventCodec : EventCodec {
    override fun decode(eventAsString: String): Event {
        return faultEventJson.decodeFromString<FaultEvent>(eventAsString)
    }

    override fun encode(event: Event): String {
        require(event is FaultEvent) {
            "TracerEventCodec can only encode TracerEvent instances, but received ${event::class.qualifiedName}"
        }

        return faultEventJson.encodeToString(event)
    }
}
