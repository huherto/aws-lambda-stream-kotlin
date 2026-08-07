package io.github.huherto.awsLambdaStream.serialization

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*

fun RecordPair.toJsonString(): String =
    Json.encodeToString(JsonObject.serializer(), toJsonObject())

fun RecordPair.toJsonObject(): JsonObject =
    buildJsonObject {
        put("new", new?.toJsonObject() ?: JsonNull)
        put("old", old?.toJsonObject() ?: JsonNull)
    }

fun RecordImage.toJsonObject(): JsonObject =
    buildJsonObject {
        this@toJsonObject.forEach { (key, value) ->
            put(key, value?.toJsonElement() ?: JsonNull)
        }
    }

fun AttributeValue.toJsonElement(): JsonElement {
    val stringValue = s
    val numberValue = n
    val booleanValue = bool
    val nullValue = null
    val mapValue = m
    val listValue = l
    val stringSetValue = ss
    val numberSetValue = ns

    return when {
        stringValue != null -> JsonPrimitive(stringValue)

        numberValue != null -> {
            val numericValue = numberValue.toDoubleOrNull()
            if (numericValue != null) {
                JsonPrimitive(numericValue)
            } else {
                JsonPrimitive(numberValue)
            }
        }

        booleanValue != null -> JsonPrimitive(booleanValue)

        nullValue == true -> JsonNull

        mapValue != null -> buildJsonObject {
            mapValue.forEach { (key, value) ->
                put(key, value?.toJsonElement() ?: JsonNull)
            }
        }

        listValue != null -> buildJsonArray {
            listValue.forEach { value ->
                add(value?.toJsonElement() ?: JsonNull)
            }
        }

        stringSetValue != null -> buildJsonArray {
            stringSetValue.forEach { value ->
                add(JsonPrimitive(value))
            }
        }

        numberSetValue != null -> buildJsonArray {
            numberSetValue.forEach { value ->
                val numericValue = value.toDoubleOrNull()
                if (numericValue != null) {
                    add(JsonPrimitive(numericValue))
                } else {
                    add(JsonPrimitive(value))
                }
            }
        }

        else -> JsonNull
    }
}

object RecordPairAsJsonObjectSerializer : KSerializer<Any?> {
    private val delegate = JsonObject.serializer().nullable

    override val descriptor: SerialDescriptor = delegate.descriptor

    override fun serialize(encoder: Encoder, value: Any?) {
        val recordPair = value as? RecordPair
        encoder.encodeSerializableValue(delegate, recordPair?.toJsonObject())
    }

    override fun deserialize(decoder: Decoder): Any? {
        val jsonObject = decoder.decodeSerializableValue(delegate) ?: return null
        return jsonObject.toRecordPair()
    }
}

class RecordPairJsonSerializer : JsonSerializer<Any?>() {
    override fun serialize(value: Any?, gen: JsonGenerator, serializers: SerializerProvider) {
        val recordPair = value as? RecordPair
        if (recordPair == null) {
            gen.writeNull()
            return
        }

        gen.writeRawValue(recordPair.toJsonString())
    }
}

class RecordPairJsonDeserializer : JsonDeserializer<Any?>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): Any? {
        val node = parser.codec.readTree<JsonNode>(parser) ?: return null
        return node.toRecordPair()
    }
}

private fun JsonObject.toRecordPair(): RecordPair =
    RecordPair(
        new = this["new"]?.jsonObjectOrNull()?.toRecordImage(),
        old = this["old"]?.jsonObjectOrNull()?.toRecordImage(),
    )

private fun JsonElement.jsonObjectOrNull(): JsonObject? =
    when (this) {
        is JsonObject -> this
        JsonNull -> null
        else -> null
    }

private fun JsonObject.toRecordImage(): RecordImage =
    RecordImage(
        mapValues { (_, value) -> value.toEventAttributeValue() }
    )

private fun JsonElement.toEventAttributeValue(): AttributeValue =
    when (this) {
        JsonNull -> AttributeValue().withNULL(true)
        is JsonPrimitive -> toEventAttributeValue()
        is JsonObject -> AttributeValue().withM(mapValues { (_, value) -> value.toEventAttributeValue() })
        is JsonArray -> AttributeValue().withL(map { it.toEventAttributeValue() })
    }

private fun JsonPrimitive.toEventAttributeValue(): AttributeValue {
    if (isString) return AttributeValue().withS(content)
    booleanOrNull?.let { return AttributeValue().withBOOL(it) }

    val numericContent = contentOrNull
    if (numericContent != null) {
        return AttributeValue().withN(numericContent)
    }

    throw SerializationException("Unsupported DynamoDB AttributeValue primitive: $this")
}

private fun JsonNode.toRecordPair(): RecordPair =
    RecordPair(
        new = get("new")?.takeUnless { it.isNull }?.toRecordImage(),
        old = get("old")?.takeUnless { it.isNull }?.toRecordImage(),
    )

private fun JsonNode.toRecordImage(): RecordImage =
    RecordImage(
        fields().asSequence().associate { (key, value) ->
            key to value.toEventAttributeValue()
        }
    )

private fun JsonNode.toEventAttributeValue(): AttributeValue =
    when {
        isNull -> AttributeValue().withNULL(true)
        isTextual -> AttributeValue().withS(asText())
        isBoolean -> AttributeValue().withBOOL(asBoolean())
        isNumber -> AttributeValue().withN(asText())
        isObject -> AttributeValue().withM(
            fields().asSequence().associate { (key, value) ->
                key to value.toEventAttributeValue()
            }
        )
        isArray -> AttributeValue().withL(
            elements().asSequence().map { it.toEventAttributeValue() }.toList()
        )
        else -> AttributeValue().withNULL(true)
    }