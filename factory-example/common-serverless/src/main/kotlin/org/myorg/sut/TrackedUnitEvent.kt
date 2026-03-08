package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

@Serializable
sealed class TrackedUnitEvent() : Event {

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

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String {
        return this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"
    }

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
class ShipmentCreatedEvent : TrackedUnitEvent()

@Serializable
@SerialName("SHIPMENT_PICKED_UP")
class ShipmentPickedUpEvent(var carrierName: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("SHIPMENT_IN_TRANSIT")
class ShipmentInTransitEvent : TrackedUnitEvent()

@Serializable
@SerialName("ARRIVAL_AT_HUB")
class ArrivalAtHubEvent(var hubId: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("DEPARTURE_FROM_HUB")
class DepartureFromHubEvent(var nextDestination: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("CUSTOMS_CLEARED")
class CustomsClearedEvent(var countryCode: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("OUT_FOR_DELIVERY")
class OutForDeliveryEvent(var estimatedArrival: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("DELIVERY_ATTEMPTED")
class DeliveryAttemptedEvent(var reason: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("SHIPMENT_DELIVERED")
class ShipmentDeliveredEvent(var signedBy: String? = null) : TrackedUnitEvent()

@Serializable
@SerialName("SHIPMENT_EXCEPTION")
class ShipmentExceptionEvent(var exceptionType: String? = null, var description: String? = null) : TrackedUnitEvent()

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    isLenient = true
}