package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvelopeEncryptionMetadata
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventReference
import io.github.huherto.awsLambdaStream.RawRecord
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
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

    abstract override val eem: EnvelopeEncryptionMetadata?

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

    abstract override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: EnvelopeEncryptionMetadata?,
        triggers: List<EventReference>?
    ): Event
}

@Serializable
@SerialName(TrackedUnitEvent.SHIPMENT_CREATED)
data class ShipmentCreatedEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.SHIPMENT_PICKED_UP)
data class ShipmentPickedUpEvent(
    val carrierName: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.SHIPMENT_IN_TRANSIT)
data class ShipmentInTransitEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.ARRIVAL_AT_HUB)
data class ArrivalAtHubEvent(
    val hubId: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.DEPARTURE_FROM_HUB)
data class DepartureFromHubEvent(
    val nextDestination: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.CUSTOMS_CLEARED)
data class CustomsClearedEvent(
    val countryCode: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.OUT_FOR_DELIVERY)
data class OutForDeliveryEvent(
    val estimatedArrival: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.DELIVERY_ATTEMPTED)
data class DeliveryAttemptedEvent(
    val reason: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.SHIPMENT_DELIVERED)
data class ShipmentDeliveredEvent(
    val signedBy: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.SHIPMENT_EXCEPTION)
data class ShipmentExceptionEvent(
    val exceptionType: String? = null,
    val description: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.VERIFY_TARGET_ADDRESS)
data class VerifyTargetAddressEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.CONTACT_CUSTOMER)
data class ContactCustomerEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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

@Serializable
@SerialName(TrackedUnitEvent.POISON_PILL_EVENT)
data class PoisonPillEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
    override val entity: TrackedUnit? = null,
    override val location: String? = null,
    override val result: String? = null
) : TrackedUnitEvent() {
    override fun toString() = super.toString()

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
