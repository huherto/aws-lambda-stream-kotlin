package org.myorg.sut

import io.github.huherto.`aws-lambda-stream`.Event
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

@Serializable
sealed class TrackedUnitEvent(@Transient override var type: String = "") : Event {

    companion object {
        fun fromString(s: String): TrackedUnitEvent {
            return jsonDecode(s)
        }
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

    override fun encoded(): String {
        return jsonEncode(this)
    }

    var location : String? = null
    var result : String? = null

    override fun toString(): String {
        return encoded()
    }
}

fun jsonEncode(event : TrackedUnitEvent) : String {
    return sutJson.encodeToString(TrackedUnitEvent.serializer(), event)
}

fun jsonDecode(s: String): TrackedUnitEvent {
    return sutJson.decodeFromString(TrackedUnitEvent.serializer(), s)
}

// Not sure if we need this
fun utf8Encode(s : String) : String {
    return StandardCharsets.UTF_8.encode(s).toString()
}

// .. or this.
fun utf8Decode(bb : ByteBuffer?) : String {
    if (bb == null) return ""
    return StandardCharsets.UTF_8.decode(bb).toString()
}

@Serializable()
@SerialName("SHIPMENT_CREATED")
class ShipmentCreatedEvent : TrackedUnitEvent("SHIPMENT_CREATED") {
}

@Serializable
@SerialName("SHIPMENT_PICKED_UP")
class ShipmentPickedUpEvent(var carrierName: String? = null) : TrackedUnitEvent("SHIPMENT_PICKED_UP") {
}

@Serializable
@SerialName("SHIPMENT_IN_TRANSIT")
class ShipmentInTransitEvent : TrackedUnitEvent("SHIPMENT_IN_TRANSIT") {
}

@Serializable
@SerialName("ARRIVAL_AT_HUB")
class ArrivalAtHubEvent(var hubId: String? = null) : TrackedUnitEvent("ARRIVAL_AT_HUB") {
}

@Serializable
@SerialName("DEPARTURE_FROM_HUB")
class DepartureFromHubEvent(var nextDestination: String? = null) : TrackedUnitEvent("DEPARTURE_FROM_HUB") {
}

@Serializable
@SerialName("CUSTOMS_CLEARED")
class CustomsClearedEvent(var countryCode: String? = null) : TrackedUnitEvent("CUSTOMS_CLEARED") {
}

@Serializable
@SerialName("OUT_FOR_DELIVERY")
class OutForDeliveryEvent(var estimatedArrival: String? = null) : TrackedUnitEvent("OUT_FOR_DELIVERY") {
}

@Serializable
@SerialName("DELIVERY_ATTEMPTED")
class DeliveryAttemptedEvent(var reason: String? = null) : TrackedUnitEvent("DELIVERY_ATTEMPTED") {
}

@Serializable
@SerialName("SHIPMENT_DELIVERED")
class ShipmentDeliveredEvent(var signedBy: String? = null) : TrackedUnitEvent("SHIPMENT_DELIVERED") {
}

@Serializable
@SerialName("SHIPMENT_EXCEPTION")
class ShipmentExceptionEvent(var exceptionType: String? = null, var description: String? = null) : TrackedUnitEvent("SHIPMENT_EXCEPTION") {
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    isLenient = true
}