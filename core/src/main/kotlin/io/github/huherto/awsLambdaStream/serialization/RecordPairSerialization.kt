package io.github.huherto.awsLambdaStream.serialization

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.serialization.json.*

/**
 * Plain-JSON views of a DynamoDB image, with the `S`/`N`/`SS` type envelope flattened away for
 * readability.
 *
 * These are lossy and one-way: sets collapse to lists and numbers are parsed, so large integers
 * lose precision. Use them for logging and human-facing output. For anything that has to survive
 * a round trip — replay, persistence — use the canonical form in [toCanonicalJsonObject].
 */
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
    val nullValue = isNULL
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

