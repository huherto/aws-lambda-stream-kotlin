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
    override var id: String?
        get() = jsonObject.stringOrNull("id")
        set(value) {}
    override var timestamp: Long?
        get() = jsonObject.longOrNull("timestamp")
        set(value) {}
    override var partitionKey: String?
        get() = jsonObject.stringOrNull("partitionKey")
        set(value) {}
    override var tags: Map<String, String>?
        get() = jsonObject.stringMapOrNull("tags")
        set(value) {}
    override var raw: Any?
        get() = jsonObject["raw"]
        set(value) {}
    override var eem: Any?
        get() = jsonObject.stringMapOrNull("eem")
        set(value) {}
    override var triggers: List<EventReference>?
        get() = (jsonObject["triggers"] as? JsonArray)
            ?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull }
            ?.map { EventReference(it) }
        set(value) { }

    fun jsonObject(path: String) : JsonObject? {
        return getJsonObjectByPath(jsonObject, path)
    }

    fun jsonPrimitive(path: String) : JsonPrimitive? {
        return getJsonPrimitiveByPath(jsonObject, path)
    }

    override fun eventType(): String {
        return jsonObject.stringOrNull("type") ?: "unknown"
        return "unknown"
    }

    override fun encoded(): String {
        return jsonObject.toString()
    }

    override fun toString(): String {
        return jsonObject.toString()
    }
}

object JsonEventCodec : EventCodec {

    override fun decode(eventAsString: String): Event {
        return JsonEvent(eventAsString)
    }

    override fun encode(event: Event): String {
        return event.encoded()
    }
}