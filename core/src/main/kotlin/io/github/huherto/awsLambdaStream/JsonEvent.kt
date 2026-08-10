package io.github.huherto.awsLambdaStream

import kotlinx.serialization.json.*

fun getJsonElementByPath(jsonObject: JsonObject, path: String): JsonElement? {
    return path.split(".").fold(jsonObject as JsonElement?) { current, key ->
        (current as? JsonObject)?.get(key)
    }
}
fun getJsonObjectByPath(jsonObject: JsonObject, path: String): JsonObject? {
    return getJsonElementByPath(jsonObject, path) as? JsonObject
}

fun getJsonPrimitiveByPath(jsonObject: JsonObject, path: String): JsonPrimitive? {
    return getJsonElementByPath(jsonObject, path) as? JsonPrimitive
}

fun JsonObject.stringOrNull(name: String): String? {
    return (this[name] as? JsonPrimitive)?.contentOrNull
}

fun JsonObject.longOrNull(name: String): Long? {
    return (this[name] as? JsonPrimitive)?.longOrNull
}

fun JsonObject.stringMapOrNull(name: String): Map<String, String>? {
    return (this[name] as? JsonObject)
        ?.mapValues { (_, value) -> (value as? JsonPrimitive)?.contentOrNull ?: return null }
}

class JsonEvent(jsonString: String) : Event {
    private val jsonObject = Json.parseToJsonElement(jsonString).jsonObject
    override val id: String?
        get() = jsonObject.stringOrNull("id")
    override val timestamp: Long?
        get() = jsonObject.longOrNull("timestamp")
    override val partitionKey: String?
        get() = jsonObject.stringOrNull("partitionKey")
    override val tags: Map<String, String>?
        get() = jsonObject.stringMapOrNull("tags")
    override val raw: RawRecord?
        get() = jsonObject["raw"]?.takeUnless { it is JsonNull }?.toRawRecord()
    override val eem: Any?
        get() = jsonObject.stringMapOrNull("eem")
    override val triggers: List<EventReference>?
        get() = (jsonObject["triggers"] as? JsonArray)
            ?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull }
            ?.map { EventReference(it) }

    fun jsonObject(path: String) : JsonObject? {
        return getJsonObjectByPath(jsonObject, path)
    }

    fun jsonPrimitive(path: String) : JsonPrimitive? {
        return getJsonPrimitiveByPath(jsonObject, path)
    }

    override fun copyEvent(
        id: String?,
        timestamp: Long?,
        partitionKey: String?,
        tags: Map<String, String>?,
        raw: RawRecord?,
        eem: Any?,
        triggers: List<EventReference>?
    ): Event {
        val map = jsonObject.toMutableMap()
        id?.let { map["id"] = JsonPrimitive(it) }
        timestamp?.let { map["timestamp"] = JsonPrimitive(it) }
        partitionKey?.let { map["partitionKey"] = JsonPrimitive(it) }
        tags?.let {
            map["tags"] = JsonObject(it.mapValues { (_, v) -> JsonPrimitive(v) })
        }
        // eem and triggers are still loosely typed, so they need runtime checks.
        if (raw != null) map["raw"] = raw.toJsonElement()
        if (eem is JsonObject) map["eem"] = eem
        if (triggers != null) {
            map["triggers"] = JsonArray(triggers.map { JsonPrimitive(it.id) }) // Simplified
        }

        return JsonEvent(JsonObject(map).toString())
    }

    override fun eventType(): String {
        return jsonObject.stringOrNull("type") ?: "unknown"
    }

    override fun toString(): String {
        return jsonObject.toString()
    }

    fun encodeWithOmit(vararg keys: String): String {
        val map = jsonObject.toMutableMap()
        keys.forEach { key ->
            map.remove(key)
        }
        val filteredJsonObject = JsonObject(map)
        return Json.encodeToString(filteredJsonObject)
    }
}

object JsonEventCodec : EventCodec {

    override fun decode(eventAsString: String): Event {
        return JsonEvent(eventAsString)
    }

    override fun encode(event: Event): String {
        return event.toString()
    }
}