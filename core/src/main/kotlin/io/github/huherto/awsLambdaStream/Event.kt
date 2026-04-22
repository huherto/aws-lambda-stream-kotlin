package io.github.huherto.awsLambdaStream

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

data class EventReference(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
)

interface EventCodec {
    fun decode(text: String): Event
    fun encode(value: Event): String
}

// Base class for all events.
abstract class BaseEvent : Event {
    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()
    override var raw: Any? = null
    override var eem: Any? = null
    override var triggers: List<EventReference>? = null
}

class FaultException(
    val uow: UnitOfWork,
    cause: Throwable?
) : RuntimeException( cause)

const val FAULT_EVENT_TYPE : String = "fault"

class FaultEvent() : BaseEvent() {

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