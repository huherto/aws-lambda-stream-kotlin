package io.github.huherto.awsLambdaStream.serialization.aws

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.util.*

val AwsJson = Json {
    encodeDefaults = false
    ignoreUnknownKeys = true
    explicitNulls = false
}

object ByteBufferSerializer : KSerializer<ByteBuffer> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("ByteBuffer", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ByteBuffer) {
        val bytes = ByteArray(value.remaining())
        value.duplicate().get(bytes)
        encoder.encodeString(Base64.getEncoder().encodeToString(bytes))
    }

    override fun deserialize(decoder: Decoder): ByteBuffer {
        val bytes = Base64.getDecoder().decode(decoder.decodeString())
        return ByteBuffer.wrap(bytes)
    }
}

object DateSerializer : KSerializer<Date> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Date", PrimitiveKind.DOUBLE)

    override fun serialize(encoder: Encoder, value: Date) {
        encoder.encodeDouble(value.time.toDouble() / 1000.0)
    }

    override fun deserialize(decoder: Decoder): Date {
        return Date((decoder.decodeDouble() * 1000.0).toLong())
    }
}
