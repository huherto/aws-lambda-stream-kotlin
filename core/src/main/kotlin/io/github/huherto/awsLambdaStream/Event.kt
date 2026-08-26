package io.github.huherto.awsLambdaStream

import kotlinx.serialization.Serializable

interface Event {
    val id: String?
    val timestamp: Long? // In milliseconds since epoch
    val partitionKey: String?
    val tags: Map<String, String>?
    val raw: RawRecord?

    // Envelope Encryption Metadata. See SAP4SS page 347
    val eem: Any?

    // References to the events that triggered this event. Useful for diagnostics.
    val triggers: List<EventReference>?

    fun eventType(): String

    fun copyEvent(
        id: String? = this.id,
        timestamp: Long? = this.timestamp,
        partitionKey: String? = this.partitionKey,
        tags: Map<String, String>? = this.tags,
        raw: RawRecord? = this.raw,
        eem: Any? = this.eem,
        triggers: List<EventReference>? = this.triggers
    ): Event
}

@Serializable
data class EventReference(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
)

abstract class BaseEvent : Event {

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: Any?,
        triggers: List<EventReference>?
    ): Event {
        return io.github.huherto.awsLambdaStream.utils.copyWithOverrides(
            this,
            mapOf(
                "id" to id,
                "timestamp" to timestamp,
                "partitionKey" to partitionKey,
                "tags" to tags,
                "raw" to raw,
                "eem" to eem,
                "triggers" to triggers
            )
        )
    }
}

@Serializable
class FaultException : RuntimeException {

    @kotlinx.serialization.Transient
    var uow: UnitOfWork? = null

    constructor(uow: UnitOfWork?, cause: Throwable?) : super(cause) {
        this.uow = uow
    }

    constructor(uow: UnitOfWork?, message: String?, cause: Throwable?,enableSuppression: Boolean,
                writableStackTrace: Boolean) : super(message, cause, enableSuppression, writableStackTrace) {
        this.uow = uow
    }
}

const val FAULT_EVENT_TYPE : String = "fault"