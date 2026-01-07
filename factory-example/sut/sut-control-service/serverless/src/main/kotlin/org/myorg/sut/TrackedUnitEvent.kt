package org.myorg.sut

import io.github.huherto.`aws-lambda-stream`.Event
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass


@Serializable
open class TrackedUnitEvent() : Event {

    override var id: String? = null
    override var type: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = HashMap<String, String>()
    var entity: TrackedUnit? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null

    override fun encoded(): String {
        return sutJson.encodeToString(this)
    }

    var location : String? = null
    var result : String? = null

    override fun toString(): String {
        return encoded()
    }
}

@Serializable
class ShipmentCreatedEvent : TrackedUnitEvent() {
    init { type = "SHIPMENT_CREATED" }
}

@Serializable
class ShipmentPickedUpEvent(var carrierName: String? = null) : TrackedUnitEvent() {
    init { type = "SHIPMENT_PICKED_UP" }
}

@Serializable
class ShipmentInTransitEvent : TrackedUnitEvent() {
    init { type = "SHIPMENT_IN_TRANSIT" }
}

@Serializable
class ArrivalAtHubEvent(var hubId: String? = null) : TrackedUnitEvent() {
    init { type = "ARRIVAL_AT_HUB" }
}

@Serializable
class DepartureFromHubEvent(var nextDestination: String? = null) : TrackedUnitEvent() {
    init { type = "DEPARTURE_FROM_HUB" }
}

@Serializable
class CustomsClearedEvent(var countryCode: String? = null) : TrackedUnitEvent() {
    init { type = "CUSTOMS_CLEARED" }
}

@Serializable
class OutForDeliveryEvent(var estimatedArrival: String? = null) : TrackedUnitEvent() {
    init { type = "OUT_FOR_DELIVERY" }
}

@Serializable
class DeliveryAttemptedEvent(var reason: String? = null) : TrackedUnitEvent() {
    init { type = "DELIVERY_ATTEMPTED" }
}

@Serializable
class ShipmentDeliveredEvent(var signedBy: String? = null) : TrackedUnitEvent() {
    init { type = "SHIPMENT_DELIVERED" }
}

@Serializable
class ShipmentExceptionEvent(var exceptionType: String? = null, var description: String? = null) : TrackedUnitEvent() {
    init { type = "SHIPMENT_EXCEPTION" }
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    serializersModule = SerializersModule {
        polymorphic(Event::class) {
            subclass(TrackedUnitEvent::class)
            subclass(ShipmentCreatedEvent::class)
            subclass(ShipmentPickedUpEvent::class)
            subclass(ShipmentInTransitEvent::class)
            subclass(ArrivalAtHubEvent::class)
            subclass(DepartureFromHubEvent::class)
            subclass(CustomsClearedEvent::class)
            subclass(OutForDeliveryEvent::class)
            subclass(DeliveryAttemptedEvent::class)
            subclass(ShipmentDeliveredEvent::class)
            subclass(ShipmentExceptionEvent::class)
        }
    }
}