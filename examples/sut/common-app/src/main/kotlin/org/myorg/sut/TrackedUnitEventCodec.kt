package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec

object TrackedUnitEventCodec : EventCodec {

    override fun decode(text: String): Event {
        return sutJson.decodeFromString(TrackedUnitEvent.serializer(), text)
    }

    override fun encode(value: Event): String {
        return sutJson.encodeToString(TrackedUnitEvent.serializer(), value as TrackedUnitEvent)
    }
}