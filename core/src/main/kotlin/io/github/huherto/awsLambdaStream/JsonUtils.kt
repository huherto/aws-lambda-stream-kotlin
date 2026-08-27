package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import kotlinx.serialization.json.*
import java.nio.ByteBuffer
import java.util.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * Extension function to convert any object to a [JsonElement] using reflection.
 * This provides a "zero-fail" serialization for logging arbitrary objects.
 */
fun Any?.toJsonElement(visited: MutableSet<Int> = mutableSetOf()): JsonElement {
    if (this == null) return JsonNull

    // Basic types
    when (this) {
        is JsonElement -> return this
        is String -> return JsonPrimitive(this)
        is Number -> return JsonPrimitive(this)
        is Boolean -> return JsonPrimitive(this)
        is Char -> return JsonPrimitive(this.toString())
        is Enum<*> -> return JsonPrimitive(this.name)
    }

    // Handle circular references
    val id = System.identityHashCode(this)
    if (id in visited) {
        return JsonPrimitive("[Circular Reference to ${this::class.simpleName}]")
    }
    visited.add(id)

    try {
        // Special cases
        when (this) {
            is ByteBuffer -> {
                val duplicate = this.duplicate()
                val bytes = ByteArray(duplicate.remaining())
                duplicate.get(bytes)
                return JsonPrimitive(Base64.getEncoder().encodeToString(bytes))
            }
            is AttributeValue -> {
                return when {
                    s != null -> JsonPrimitive(s)
                    n != null -> n.toJsonNumber()
                    b != null -> b.toJsonElement(visited)
                    getBOOL() != null -> JsonPrimitive(getBOOL())
                    getNULL() == true -> JsonNull
                    m != null -> m.toJsonElement(visited)
                    l != null -> l.toJsonElement(visited)
                    getSS() != null -> JsonArray(getSS().map { JsonPrimitive(it) })
                    getNS() != null -> JsonArray(getNS().map { it.toJsonNumber() })
                    getBS() != null -> JsonArray(getBS().map { it.toJsonElement(visited) })
                    else -> JsonNull
                }
            }
            is Pipeline -> {
                return buildJsonObject {
                    put("id", id)
                }
            }
        }

        // Iterables & Arrays
        if (this is Iterable<*>) {
            return JsonArray(this.map { it.toJsonElement(visited) })
        }
        if (this::class.java.isArray) {
            val list = mutableListOf<JsonElement>()
            for (i in 0 until java.lang.reflect.Array.getLength(this)) {
                list.add(java.lang.reflect.Array.get(this, i).toJsonElement(visited))
            }
            return JsonArray(list)
        }

        // Maps
        if (this is Map<*, *>) {
            return buildJsonObject {
                this@toJsonElement.forEach { (key, value) ->
                    put(key.toString(), value.toJsonElement(visited))
                }
            }
        }

        // Generic objects via reflection
        return ReflectionSupport.toJsonElement(this, visited)
    } finally {
        visited.remove(id)
    }
}

private object ReflectionSupport {
    val isKotlinReflectAvailable: Boolean by lazy {
        try {
            Class.forName("kotlin.reflect.full.KClasses")
            true
        } catch (e: Exception) {
            false
        }
    }

    fun toJsonElement(obj: Any, visited: MutableSet<Int>): JsonElement {
        val jsonFromJava = toJsonElementJava(obj, visited)

        if (!isKotlinReflectAvailable) return jsonFromJava

        return try {
            val jsonFromKotlin = KotlinReflectionHelper.toJsonElement(obj, visited)
            if (jsonFromKotlin is JsonObject && jsonFromJava is JsonObject) {
                buildJsonObject {
                    jsonFromJava.forEach { (k, v) -> put(k, v) }
                    jsonFromKotlin.forEach { (k, v) -> put(k, v) }
                }
            } else {
                jsonFromKotlin
            }
        } catch (e: Throwable) {
            jsonFromJava
        }
    }

    private fun toJsonElementJava(obj: Any, visited: MutableSet<Int>): JsonElement {
        return try {
            val clazz = obj.javaClass
            buildJsonObject {
                // Try getters first
                clazz.methods.filter {
                    (it.name.startsWith("get") || it.name.startsWith("is")) &&
                            it.parameterCount == 0 &&
                            it.name != "getClass" &&
                            it.name != "getDeclaringClass"
                }.forEach { method ->
                    try {
                        val prefix = if (method.name.startsWith("get")) 3 else 2
                        val name = method.name.substring(prefix).replaceFirstChar { it.lowercase() }
                        if (name.isNotEmpty()) {
                            method.isAccessible = true
                            val value = method.invoke(obj)
                            put(name, value.toJsonElement(visited))
                        }
                    } catch (e: Exception) {
                    }
                }
                // Then fields if they are public
                clazz.fields.forEach { field ->
                    try {
                        field.isAccessible = true
                        val value = field.get(obj)
                        put(field.name, value.toJsonElement(visited))
                    } catch (e: Exception) {
                    }
                }
            }
        } catch (e: Throwable) {
            JsonPrimitive(obj.toString())
        }
    }
}

private object KotlinReflectionHelper {
    fun toJsonElement(obj: Any, visited: MutableSet<Int>): JsonElement {
        val properties = obj::class.memberProperties
        return buildJsonObject {
            properties.forEach { prop ->
                try {
                    prop.isAccessible = true
                    val value = prop.call(obj)
                    put(prop.name, value.toJsonElement(visited))
                } catch (e: Exception) {
                    // Skip properties that can't be accessed or called
                }
            }
        }
    }
}

private fun String.toJsonNumber(): JsonPrimitive {
    return this.toLongOrNull()?.let { JsonPrimitive(it) }
        ?: this.toDoubleOrNull()?.let { JsonPrimitive(it) }
        ?: JsonPrimitive(this)
}

object SafeLogger {
    private val json = Json {
        prettyPrint = true
        encodeDefaults = true
    }

    /**
     * Converts any object to JSON string safely.
     * Never throws an exception.
     */
    fun toJson(obj: Any?): String {
        if (obj == null) return "null"

        return try {
            json.encodeToString(JsonElement.serializer(), obj.toJsonElement())
        } catch (throwable: Throwable) {
            // FALLBACK: Return a JSON-safe error descriptor
            val className = obj::class.java.name
            val toStringVal = try { obj.toString() } catch (e: Exception) { "[toString() failed]" }

            """{"log_error": "Serialization failed", "class": "$className", "toString": "${toStringVal.escapeJson()}", "message": "${throwable.message?.escapeJson()}"}"""
        }
    }

    // Helper to prevent broken JSON in the fallback message
    private fun String.escapeJson() = this.replace("\"", "\\\"").replace("\n", "\\n")
}