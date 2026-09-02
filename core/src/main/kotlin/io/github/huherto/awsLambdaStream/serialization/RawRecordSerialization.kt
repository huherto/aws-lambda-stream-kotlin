package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.from.RecordImage
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonObject

/** Serializes a [RecordImage] as canonical DynamoDB JSON. */
object RecordImageSerializer : KSerializer<RecordImage> {
    private val delegate = JsonObject.serializer()

    override val descriptor: SerialDescriptor = delegate.descriptor

    override fun serialize(encoder: Encoder, value: RecordImage) {
        encoder.encodeSerializableValue(delegate, value.map.toCanonicalJsonObject())
    }

    override fun deserialize(decoder: Decoder): RecordImage {
        return RecordImage(decoder.decodeSerializableValue(delegate).toCanonicalAttributeValueMap())
    }
}
