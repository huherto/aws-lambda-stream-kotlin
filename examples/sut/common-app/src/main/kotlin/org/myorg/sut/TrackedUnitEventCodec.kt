package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import kotlinx.serialization.json.Json

object TrackedUnitEventCodec : EventCodec {

    override fun decode(text: String): Event {
        return sutJson.decodeFromString(TrackedUnitEvent.serializer(), text)
    }

    override fun encode(value: Event): String {
        return sutJson.encodeToString(TrackedUnitEvent.serializer(), value as TrackedUnitEvent)
    }
}

val sutJson: Json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
    isLenient = true
}