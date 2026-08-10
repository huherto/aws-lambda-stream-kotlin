package io.github.huherto.awsLambdaStream

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.github.huherto.awsLambdaStream.serialization.JsonElementJacksonDeserializer
import io.github.huherto.awsLambdaStream.serialization.JsonElementSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Type discriminators for [RawRecord]. Shared by the kotlinx `@SerialName` and the Jackson
 * `@JsonSubTypes` mappings so both formats agree on the wire.
 */
const val RAW_DYNAMODB = "dynamodb"
const val RAW_IMAGES = "images"
const val RAW_KINESIS = "kinesis"
const val RAW_SQS = "sqs"
const val RAW_CLAIM_CHECK = "claimCheck"
const val RAW_JSON = "json"

/**
 * The source record an [Event] was derived from.
 *
 * This is a closed hierarchy so that `Event.raw` serializes without `@Contextual` guesswork:
 * kotlinx resolves the variant from the `type` discriminator, and Jackson does the same through
 * [JsonTypeInfo]. Variants that wrap an AWS Lambda event record keep that record whole and
 * encode it with the AWS serializers, so a serialized event carries the exact payload Lambda
 * delivered rather than a lossy projection of it.
 *
 * [JsonRaw] is the escape hatch for payloads that have no dedicated variant.
 */
@Serializable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = DynamodbRaw::class, name = RAW_DYNAMODB),
    JsonSubTypes.Type(value = ImagesRaw::class, name = RAW_IMAGES),
    JsonSubTypes.Type(value = KinesisRaw::class, name = RAW_KINESIS),
    JsonSubTypes.Type(value = SqsRaw::class, name = RAW_SQS),
    JsonSubTypes.Type(value = ClaimCheck::class, name = RAW_CLAIM_CHECK),
    JsonSubTypes.Type(value = JsonRaw::class, name = RAW_JSON),
)
sealed interface RawRecord

/**
 * Raw payload with no dedicated variant, carried as JSON.
 */
@Serializable
@SerialName(RAW_JSON)
data class JsonRaw(
    @field:JsonSerialize(using = JsonElementSerializer::class)
    @field:JsonDeserialize(using = JsonElementJacksonDeserializer::class)
    val value: JsonElement,
) : RawRecord

private val rawRecordJson = Json { ignoreUnknownKeys = true }

/**
 * Reads a JSON `raw` value back into a [RawRecord].
 *
 * Elements carrying a known `type` discriminator round-trip to their original variant; anything
 * else — including bare JSON written by an older producer — is wrapped in [JsonRaw] rather than
 * failing.
 */
fun JsonElement.toRawRecord(): RawRecord {
    val discriminated = this as? JsonObject ?: return JsonRaw(this)
    if (discriminated["type"] !is JsonPrimitive) return JsonRaw(this)

    return runCatching {
        rawRecordJson.decodeFromJsonElement(RawRecord.serializer(), discriminated)
    }.getOrElse { JsonRaw(this) }
}

/** Encodes a [RawRecord] to its discriminated JSON form. */
fun RawRecord.toJsonElement(): JsonElement =
    rawRecordJson.encodeToJsonElement(RawRecord.serializer(), this)
