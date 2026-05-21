package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventReference
import kotlinx.serialization.Contextual
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializerOrNull

@Serializable
sealed class TrackedUnitEvent() : Event {

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

    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = HashMap<String, String>()
    var entity: TrackedUnit? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null

    @Serializable(with = EventReferenceListSerializer::class)
    override var triggers: List<EventReference>? = null

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

    override fun encoded(): String {
        return TrackedUnitEventCodec.encode(this)
    }

    var location : String? = null
    var result : String? = null

    override fun toString(): String {
        return encoded()
    }

}

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_CREATED)
class ShipmentCreatedEvent : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_PICKED_UP)
class ShipmentPickedUpEvent(var carrierName: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_IN_TRANSIT)
class ShipmentInTransitEvent : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.ARRIVAL_AT_HUB)
class ArrivalAtHubEvent(var hubId: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.DEPARTURE_FROM_HUB)
class DepartureFromHubEvent(var nextDestination: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.CUSTOMS_CLEARED)
class CustomsClearedEvent(var countryCode: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.OUT_FOR_DELIVERY)
class OutForDeliveryEvent(var estimatedArrival: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.DELIVERY_ATTEMPTED)
class DeliveryAttemptedEvent(var reason: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_DELIVERED)
class ShipmentDeliveredEvent(var signedBy: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.SHIPMENT_EXCEPTION)
class ShipmentExceptionEvent(var exceptionType: String? = null, var description: String? = null) : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.VERIFY_TARGET_ADDRESS)
class VerifyTargetAddressEvent() : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.CONTACT_CUSTOMER)
class ContactCustomerEvent() : TrackedUnitEvent()

@Serializable
@kotlinx.serialization.SerialName(TrackedUnitEvent.POISON_PILL_EVENT)
class PoisonPillEvent() : TrackedUnitEvent()
