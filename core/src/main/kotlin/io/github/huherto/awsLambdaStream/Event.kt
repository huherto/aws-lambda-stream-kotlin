package io.github.huherto.awsLambdaStream

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

interface Event {
    var id: String?
    var timestamp: Long? // In milliseconds since epoch
    var partitionKey: String?
    var tags: Map<String, String>?
    var raw: Any?

    // Envelope Encryption Metadata. See SAP4SS page 347
    var eem: Any?

    // References to the events that triggered this event. Useful for diagnostics.
    var triggers: List<EventReference>?

    fun eventType(): String

    fun encoded()  : String
}

@Serializable
data class EventReference(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
)

@Serializable
abstract class BaseEvent : Event {
    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null
    override var triggers: List<EventReference>? = null
}

class FaultException : RuntimeException {
    val uow: UnitOfWork

    constructor(uow: UnitOfWork, cause: Throwable?) : super(cause) {
        this.uow = uow
    }

    constructor(uow: UnitOfWork, message: String, cause: Throwable?,enableSuppression: Boolean,
                writableStackTrace: Boolean) : super(message, cause, enableSuppression, writableStackTrace) {
        this.uow = uow
    }
}

const val FAULT_EVENT_TYPE : String = "fault"

@Serializable
class FaultEvent() : BaseEvent() {

    @Contextual
    var failureException: FaultException? = null

    override fun eventType(): String {
        return FAULT_EVENT_TYPE
    }

    override fun toString(): String {
        return failureException?.cause?.message ?: "Unknown failure"
    }

    override fun encoded(): String {
        return this.asJson()
    }
}