package io.kopipes.core

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import java.util.concurrent.ConcurrentHashMap

const val RAW_DYNAMODB = "dynamodb"
const val RAW_IMAGES = "images"
const val RAW_KINESIS = "kinesis"
const val RAW_SQS = "sqs"
const val RAW_CLAIM_CHECK = "claimCheck"
const val RAW_JSON = "json"

/** The source record an [Event] was derived from. */
@Serializable(with = RawRecordSerializer::class)
interface RawRecord

/** Raw payload with no dedicated variant, carried as JSON. */
@Serializable
@SerialName(RAW_JSON)
data class JsonRaw(
    val value: JsonElement,
) : RawRecord

@OptIn(InternalSerializationApi::class)
object RawRecordSerializer : KSerializer<RawRecord> {
    private val serializers = ConcurrentHashMap<String, KSerializer<out RawRecord>>()

    fun register(name: String, serializer: KSerializer<out RawRecord>) {
        serializers[name] = serializer
        serializers[serializer.descriptor.serialName] = serializer
    }

    init {
        register(RAW_JSON, JsonRaw.serializer())
    }

    override val descriptor: SerialDescriptor = JsonElement.serializer().descriptor

    @Suppress("UNCHECKED_CAST")
    override fun serialize(encoder: Encoder, value: RawRecord) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: throw IllegalStateException("RawRecordSerializer only supports JSON")
        val serializer = serializers.values.find {
            it.descriptor.serialName == value::class.qualifiedName ||
            it.descriptor.serialName == value::class.simpleName
        } ?: (value::class.serializerOrNull() as? KSerializer<out RawRecord>)
          ?: (if (value is JsonRaw) JsonRaw.serializer() else null)

        if (serializer != null) {
            val jsonElement = jsonEncoder.json.encodeToJsonElement(serializer as KSerializer<RawRecord>, value)
            if (jsonElement is JsonObject && !jsonElement.containsKey("type")) {
                val withType = buildJsonObject {
                    put("type", serializer.descriptor.serialName)
                    jsonElement.forEach { (k, v) -> put(k, v) }
                }
                jsonEncoder.encodeJsonElement(withType)
            } else {
                jsonEncoder.encodeJsonElement(jsonElement)
            }
        } else {
            throw IllegalStateException("No serializer registered for ${value::class}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(decoder: Decoder): RawRecord {
        val jsonDecoder = decoder as? JsonDecoder
            ?: throw IllegalStateException("RawRecordSerializer only supports JSON")
        val element = jsonDecoder.decodeJsonElement()
        val type = (element as? JsonObject)?.get("type")?.let { (it as? JsonPrimitive)?.content }
        val serializer = serializers[type]
        return if (serializer != null) {
            val withoutType = (element as? JsonObject)?.toMutableMap()?.apply { remove("type") }?.let { JsonObject(it) } ?: element
            jsonDecoder.json.decodeFromJsonElement(serializer as KSerializer<RawRecord>, withoutType)
        } else {
            JsonRaw(element)
        }
    }
}

private val rawRecordJson = Json { ignoreUnknownKeys = true }

fun JsonElement.toRawRecord(): RawRecord {
    val discriminated = this as? JsonObject ?: return JsonRaw(this)
    if (discriminated["type"] !is JsonPrimitive) return JsonRaw(this)

    return runCatching {
        rawRecordJson.decodeFromJsonElement(RawRecordSerializer, discriminated)
    }.getOrElse { JsonRaw(this) }
}

fun RawRecord.toJsonElement(): JsonElement =
    rawRecordJson.encodeToJsonElement(RawRecordSerializer, this)
