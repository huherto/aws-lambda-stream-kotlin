package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventReference
import io.github.huherto.awsLambdaStream.RawRecord
import kotlinx.serialization.Contextual
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializerOrNull

@Serializable
sealed class TrackedUnitEvent : Event {

    companion object {
        const val SHIPMENT_CREATED = "SHIPMENT_CREATED"
        const val SHIPMENT_PICKED_UP = "SHIPMENT_PICKED_UP"
        const val SHIPMENT_IN_TRANSIT = "SHIPMENT_IN_TRANSIT"
        const val ARRIVAL_AT_HUB = "ARRIVAL_AT_HUB"
        const val DEPARTURE_FROM_HUB = "DEPARTURE_FROM_HUB"
        const val CUSTOMS_CLEARED = "CUSTOMS_CLEARED"
        const val OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
        const val DELIVERY_ATTEMPTED = "DELIVERY_ATTEMPTED"
        const val SHIPMENT_DELIVERED = "SHIPMENT_DELIVERED"
        const val SHIPMENT_EXCEPTION = "SHIPMENT_EXCEPTION"
        const val VERIFY_TARGET_ADDRESS = "VERIFY_TARGET_ADDRESS"
        const val CONTACT_CUSTOMER = "CONTACT_CUSTOMER"
        const val POISON_PILL_EVENT = "POISON_PILL_EVENT"
    }

    abstract override val id: String?
    abstract override val timestamp: Long?
    abstract override val partitionKey: String?
    abstract override val tags: Map<String, String>?

    abstract override val raw: RawRecord?

    @Contextual
    abstract override val eem: Any?

    @Serializable(with = EventReferenceListSerializer::class)
    abstract override val triggers: List<EventReference>?

    abstract val entity: TrackedUnit?
    abstract val location : String?
    abstract val result : String?

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

    override fun toString(): String {
        return TrackedUnitEventCodec.encode(this)
    }

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
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_CREATED)
data class ShipmentCreatedEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_PICKED_UP)
data class ShipmentPickedUpEvent(
    val carrierName: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_IN_TRANSIT)
data class ShipmentInTransitEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.ARRIVAL_AT_HUB)
data class ArrivalAtHubEvent(
    val hubId: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.DEPARTURE_FROM_HUB)
data class DepartureFromHubEvent(
    val nextDestination: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.CUSTOMS_CLEARED)
data class CustomsClearedEvent(
    val countryCode: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.OUT_FOR_DELIVERY)
data class OutForDeliveryEvent(
    val estimatedArrival: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.DELIVERY_ATTEMPTED)
data class DeliveryAttemptedEvent(
    val reason: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_DELIVERED)
data class ShipmentDeliveredEvent(
    val signedBy: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_EXCEPTION)
data class ShipmentExceptionEvent(
    val exceptionType: String? = null,
    val description: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.VERIFY_TARGET_ADDRESS)
data class VerifyTargetAddressEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.CONTACT_CUSTOMER)
data class ContactCustomerEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.POISON_PILL_EVENT)
data class PoisonPillEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    @Contextual override val eem: Any? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()
}
