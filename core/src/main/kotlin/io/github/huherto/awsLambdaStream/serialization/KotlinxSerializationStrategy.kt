package io.github.huherto.awsLambdaStream.serialization

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import kotlinx.serialization.serializerOrNull

class KotlinxSerializationStrategy(
    private val json: Json = defaultJson()
) {

    companion object {
        fun defaultJson(): Json = Json {
            encodeDefaults = false
            ignoreUnknownKeys = true
            isLenient = true
            prettyPrint = true
        }
    }

    @Suppress("UNCHECKED_CAST")
    @OptIn(InternalSerializationApi::class)
    fun serialize(value: Any?): String {
        if (value == null) return "null"
        val serializer = value::class.serializerOrNull() ?: serializer(value::class.java)
        return json.encodeToString(serializer as KSerializer<Any>, value)
    }

    @Suppress("UNCHECKED_CAST")
    @OptIn(InternalSerializationApi::class)
    fun <T : Any> deserialize(payload: String, targetType: Class<T>): T {
        val serializer = targetType.kotlin.serializerOrNull() ?: serializer(targetType)
        return json.decodeFromString(serializer as KSerializer<T>, payload)
    }
}
