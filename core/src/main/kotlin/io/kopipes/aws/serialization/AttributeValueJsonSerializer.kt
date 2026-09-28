package io.kopipes.aws.serialization

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.kopipes.aws.CustomJsonSerializer
import io.kopipes.aws.JsonCustomSerializers
import io.kopipes.aws.toJsonElement
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

/**
 * Converts a DynamoDB [AttributeValue] to a [JsonElement].
 */
fun AttributeValue.toJsonElement(visited: MutableSet<Int> = mutableSetOf()): JsonElement {
    return when {
        s != null -> JsonPrimitive(s)
        n != null -> n.toLongOrNull()?.let { JsonPrimitive(it) } ?: n.toDoubleOrNull()?.let { JsonPrimitive(it) } ?: JsonPrimitive(n)
        b != null -> b.toJsonElement(visited)
        getBOOL() != null -> JsonPrimitive(getBOOL())
        getNULL() == true -> JsonNull
        m != null -> m.toJsonElement(visited)
        l != null -> l.toJsonElement(visited)
        getSS() != null -> JsonArray(getSS().map { JsonPrimitive(it) })
        getNS() != null -> JsonArray(getNS().map { it.toLongOrNull()?.let { num -> JsonPrimitive(num) } ?: it.toDoubleOrNull()?.let { num -> JsonPrimitive(num) } ?: JsonPrimitive(it) })
        getBS() != null -> JsonArray(getBS().map { it.toJsonElement(visited) })
        else -> JsonNull
    }
}

object AttributeValueJsonSerializer : CustomJsonSerializer {
    init {
        JsonCustomSerializers.register(this)
    }

    override fun serialize(obj: Any, visited: MutableSet<Int>): JsonElement? {
        if (obj is AttributeValue) {
            return obj.toJsonElement(visited)
        }
        return null
    }
}
