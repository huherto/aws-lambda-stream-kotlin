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

class JsonEvent(jsonString: String) : Event {

    private val jsonObject = Json.parseToJsonElement(jsonString).jsonObject
    override var id: String?
        get() = jsonObject["id"]?.jsonPrimitive?.content
        set(value) {}
    override var timestamp: Long?
        get() = jsonObject["timestamp"]?.jsonPrimitive?.long
        set(value) {}
    override var partitionKey: String?
        get() = jsonObject["partitionKey"]?.jsonPrimitive?.content
        set(value) {}
    override var tags: Map<String, String>?
        get() = jsonObject["tags"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content }
        set(value) {}
    override var raw: Any?
        get() = jsonObject["raw"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content }
        set(value) {}
    override var eem: Any?
        get() = jsonObject["eem"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content }
        set(value) {}
    override var triggers: List<EventReference>?
        get() = jsonObject["triggers"]?.jsonArray?.map { EventReference(it.jsonPrimitive.content) }
        set(value) { }

    fun jsonObject(path: String) : JsonObject? {
        return getJsonObjectByPath(jsonObject, path)
    }

    fun jsonPrimitive(path: String) : JsonPrimitive? {
        return getJsonPrimitiveByPath(jsonObject, path)
    }

    override fun eventType(): String {
        jsonObject["type"]?.jsonPrimitive?.content?.let { return it }
        return "unknown"
    }

    override fun encoded(): String {
        return jsonObject.toString()
    }
}