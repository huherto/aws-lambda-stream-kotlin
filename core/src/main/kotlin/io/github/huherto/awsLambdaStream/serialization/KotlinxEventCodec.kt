package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class KotlinxEventCodec<T : Event>(
    private val serializer: KSerializer<T>,
    private val json: Json = KotlinxSerializationStrategy.defaultJson()
) : EventCodec {
    override fun decode(eventAsString: String): Event {
        return json.decodeFromString(serializer, eventAsString)
    }

    @Suppress("UNCHECKED_CAST")
    override fun encode(event: Event): String {
        return json.encodeToString(serializer, event as T)
    }
}
