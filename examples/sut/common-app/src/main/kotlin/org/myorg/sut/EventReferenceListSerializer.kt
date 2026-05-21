package org.myorg.sut

import io.github.huherto.awsLambdaStream.EventReference
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object EventReferenceListSerializer : KSerializer<List<EventReference>?> {
    private val surrogateSerializer = ListSerializer(EventReferenceSurrogate.serializer()).nullable

    override val descriptor: SerialDescriptor = surrogateSerializer.descriptor

    override fun serialize(encoder: Encoder, value: List<EventReference>?) {
        val surrogate = value?.map { it.toSurrogate() }
        encoder.encodeSerializableValue(surrogateSerializer, surrogate)
    }

    override fun deserialize(decoder: Decoder): List<EventReference>? {
        return decoder.decodeSerializableValue(surrogateSerializer)
            ?.map { it.toEventReference() }
    }
}

@Serializable
private data class EventReferenceSurrogate(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
)

private fun EventReference.toSurrogate(): EventReferenceSurrogate =
    EventReferenceSurrogate(
        id = id,
        type = type,
        timestamp = timestamp,
    )

private fun EventReferenceSurrogate.toEventReference(): EventReference =
    EventReference(
        id = id,
        type = type,
        timestamp = timestamp,
    )