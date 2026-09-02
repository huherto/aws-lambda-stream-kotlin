package io.github.huherto.awsLambdaStream

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

const val RAW_DYNAMODB = "dynamodb"
const val RAW_IMAGES = "images"
const val RAW_KINESIS = "kinesis"
const val RAW_SQS = "sqs"
const val RAW_CLAIM_CHECK = "claimCheck"
const val RAW_JSON = "json"

/** The source record an [Event] was derived from. */
@Serializable
sealed interface RawRecord

/** Raw payload with no dedicated variant, carried as JSON. */
@Serializable
@SerialName(RAW_JSON)
data class JsonRaw(
    val value: JsonElement,
) : RawRecord

private val rawRecordJson = Json { ignoreUnknownKeys = true }

fun JsonElement.toRawRecord(): RawRecord {
    val discriminated = this as? JsonObject ?: return JsonRaw(this)
    if (discriminated["type"] !is JsonPrimitive) return JsonRaw(this)

    return runCatching {
        rawRecordJson.decodeFromJsonElement(RawRecord.serializer(), discriminated)
    }.getOrElse { JsonRaw(this) }
}

fun RawRecord.toJsonElement(): JsonElement =
    rawRecordJson.encodeToJsonElement(RawRecord.serializer(), this)
