package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import kotlinx.serialization.json.Json

object TrackedUnitEventCodec : EventCodec {

    override fun decode(eventAsString: String): Event {
        return sutJson.decodeFromString(TrackedUnitEvent.serializer(), eventAsString)
    }

    override fun encode(event: Event): String {
        return sutJson.encodeToString(TrackedUnitEvent.serializer(), event as TrackedUnitEvent)
    }
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    isLenient = true
}