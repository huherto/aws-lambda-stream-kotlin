package io.kopipes.core

import io.kopipes.core.flavors.Pipeline
import kotlinx.serialization.json.*
import java.nio.ByteBuffer
import java.util.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

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
        } catch (e: Exception) {
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
        val kClass = obj::class
        val properties = try {
            kClass.memberProperties
        } catch (e: Exception) {
            return buildJsonObject { }
        }

        return buildJsonObject {
            for (prop in properties) {
                try {
                    prop.isAccessible = true
                    val value = prop.getter.call(obj)
                    put(prop.name, value.toJsonElement(visited))
                } catch (e: Throwable) {
                    continue
                }
            }
        }
    }
}

fun String.toJsonNumber(): JsonPrimitive {
    return this.toIntOrNull()?.let { JsonPrimitive(it) }
        ?: this.toLongOrNull()?.let { JsonPrimitive(it) }
        ?: this.toDoubleOrNull()?.let { JsonPrimitive(it) }
        ?: JsonPrimitive(this)
}

object SafeLogger {
    fun toJson(obj: Any?): String {
        return obj.toJsonElement().toString()
    }
}
