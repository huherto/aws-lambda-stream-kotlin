package org.myorg.sut

import io.github.huherto.awsLambdaStream.EventReference
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*

object EventReferenceListSerializer : KSerializer<List<EventReference>?> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("EventReferenceList")

    @OptIn(ExperimentalSerializationApi::class)
    override fun serialize(encoder: Encoder, value: List<EventReference>?) {
        if (value == null) {
            encoder.encodeNull()
            return
        }

        val jsonEncoder = encoder as? JsonEncoder
            ?: throw SerializationException("EventReferenceListSerializer supports JSON only")

        val jsonArray = buildJsonArray {
            value.forEach { ref ->
                add(
                    buildJsonObject {
                        ref.id?.let { put("id", it) }
                        ref.type?.let { put("type", it) }
                        ref.timestamp?.let { put("timestamp", it) }
                    }
                )
            }
        }

        jsonEncoder.encodeJsonElement(jsonArray)
    }

    override fun deserialize(decoder: Decoder): List<EventReference>? {
        val jsonDecoder = decoder as? JsonDecoder
            ?: throw SerializationException("EventReferenceListSerializer supports JSON only")

        val element = jsonDecoder.decodeJsonElement()
        if (element is JsonNull) return null

        return element.jsonArray.map { item ->
            val obj = item.jsonObject
            EventReference(
                id = obj["id"]?.jsonPrimitive?.contentOrNull,
                type = obj["type"]?.jsonPrimitive?.contentOrNull,
                timestamp = obj["timestamp"]?.jsonPrimitive?.longOrNull
            )
        }
    }
}