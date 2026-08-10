package io.github.huherto.awsLambdaStream.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*

/**
 * Serializes a [RecordImage] as canonical DynamoDB JSON (`{"pk":{"S":"a"}}`), matching the shape
 * DynamoDB Streams delivers. See [toCanonicalJsonObject] for why the type envelope is kept.
 */
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

class RecordImageJacksonSerializer : JsonSerializer<RecordImage>() {
    override fun serialize(value: RecordImage?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }
        gen.writeRawValue(Json.encodeToString(JsonObject.serializer(), value.map.toCanonicalJsonObject()))
    }
}

class RecordImageJacksonDeserializer : JsonDeserializer<RecordImage>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): RecordImage? {
        val node = parser.codec.readTree<JsonNode>(parser) ?: return null
        if (node.isNull) return null
        return RecordImage(
            Json.parseToJsonElement(node.toString()).jsonObject.toCanonicalAttributeValueMap()
        )
    }
}

/**
 * Reads arbitrary JSON into a kotlinx [JsonElement]. The matching serializer is
 * [JsonElementSerializer], registered by [JacksonSerializationStrategy.defaultMapper].
 */
class JsonElementJacksonDeserializer : JsonDeserializer<JsonElement>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): JsonElement {
        val node = parser.codec.readTree<JsonNode>(parser) ?: return JsonNull
        return Json.parseToJsonElement(node.toString())
    }
}
