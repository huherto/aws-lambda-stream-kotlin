package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.JsonEvent
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

fun omit(event: Event, vararg keys: String): String {

    if (event is JsonEvent) return event.encodeWithOmit(*keys)

    val jsonString = event.encoded()
    val jsonElement = Json.parseToJsonElement(jsonString)
    val jsonObject = jsonElement.jsonObject

    val mutableMap = jsonObject.toMutableMap()
    keys.forEach { key ->
        mutableMap.remove(key)
    }

    val filteredJsonObject = JsonObject(mutableMap)
    return Json.encodeToString(filteredJsonObject)
}