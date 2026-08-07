package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec

class JacksonEventCodec<T : Event>(
    private val eventClass: Class<T>,
    private val serialization: SerializationStrategy = JacksonSerializationStrategy()
) : EventCodec {
    override fun decode(eventAsString: String): Event {
        return serialization.deserialize(eventAsString, eventClass)
    }

    override fun encode(event: Event): String {
        return serialization.serialize(event)
    }
}
