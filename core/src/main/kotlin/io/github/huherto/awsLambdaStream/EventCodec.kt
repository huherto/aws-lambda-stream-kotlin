package io.github.huherto.awsLambdaStream

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

interface EventCodec {
    fun decode(eventAsString: String): Event
    fun encode(event: Event): String
    fun decode(payload: ByteBuffer?): Event {
        requireNotNull(payload) { "Cannot decode null ByteBuffer payload" }

        val duplicate = payload.duplicate()
        val text = StandardCharsets.UTF_8.decode(duplicate).toString()

        return decode(text)
    }
}