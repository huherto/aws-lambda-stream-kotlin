package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import java.nio.ByteBuffer
import java.util.*

class ByteBufferSerializer : JsonSerializer<ByteBuffer>() {
    override fun serialize(value: ByteBuffer?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
        } else {
            val duplicate = value.duplicate()
            val bytes = ByteArray(duplicate.remaining())
            duplicate.get(bytes)
            gen.writeString(Base64.getEncoder().encodeToString(bytes))
        }
    }
}

class PipelineSerializer : JsonSerializer<Pipeline>() {
    override fun serialize(value: Pipeline?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }

        gen.writeStartObject()
        gen.writeObjectField("id", value.id)
        gen.writeEndObject()
    }
}

class AttributeValueSerializer : JsonSerializer<AttributeValue>() {

    override fun serialize(value: AttributeValue?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }

        // Unwraps the AWS AttributeValue into standard JSON types for cleaner logging
        when {
            value.s != null -> gen.writeString(value.s)
            value.n != null -> gen.writeNumber(value.n) // Jackson safely writes the string representation as a raw JSON number
            value.b != null -> gen.writeString(encodeByteBuffer(value.b))
            value.getBOOL() != null -> gen.writeBoolean(value.getBOOL())
            value.getNULL() == true -> gen.writeNull()
            value.m != null -> {
                gen.writeStartObject()
                value.m.forEach { (key, attrValue) ->
                    gen.writeFieldName(key)
                    // Recursively serialize nested maps
                    serialize(attrValue, gen, serializers)
                }
                gen.writeEndObject()
            }
            value.l != null -> {
                gen.writeStartArray()
                value.l.forEach { attrValue -> serialize(attrValue, gen, serializers) }
                gen.writeEndArray()
            }
            value.getSS() != null -> {
                gen.writeStartArray()
                value.getSS().forEach { gen.writeString(it) }
                gen.writeEndArray()
            }
            value.getNS() != null -> {
                gen.writeStartArray()
                value.getNS().forEach { gen.writeNumber(it) }
                gen.writeEndArray()
            }
            value.getBS() != null -> {
                gen.writeStartArray()
                value.getBS().forEach { gen.writeString(encodeByteBuffer(it)) }
                gen.writeEndArray()
            }
            else -> gen.writeNull() // Fallback if the AttributeValue is completely empty
        }
    }

    private fun encodeByteBuffer(bb: ByteBuffer): String {
        val duplicate = bb.duplicate()
        val bytes = ByteArray(duplicate.remaining())
        duplicate.get(bytes)
        return Base64.getEncoder().encodeToString(bytes)
    }
}

private val jacksonMapper = jacksonObjectMapper().apply {
    enable(SerializationFeature.INDENT_OUTPUT) // Pretty print
    disable(SerializationFeature.FAIL_ON_EMPTY_BEANS) // Prevent crashes on objects with no public properties
    setSerializationInclusion(JsonInclude.Include.NON_NULL);

    val module = SimpleModule()
    module.addSerializer(ByteBuffer::class.java, ByteBufferSerializer())
    module.addSerializer(AttributeValue::class.java, AttributeValueSerializer())
    module.addSerializer(Pipeline::class.java, PipelineSerializer())
    registerModule(module)
}

fun Any?.asJson() : String {
    if (this == null) return "null"
    return try {
        jacksonMapper.writeValueAsString(this)
    } catch (e: Exception) {
        "\"Error serializing to JSON: ${e.message}\""
    }
}

object SafeLogger {
    // Jackson module for Kotlin adds support for data classes and nullability
    private val mapper: ObjectMapper = jacksonObjectMapper().apply {
        // 1. Don't crash on empty/proxy objects
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)

        // 2. Allow access to private fields (important for non-data classes)
        setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
    }

    /**
     * Converts any object to JSON string safely.
     * Never throws an exception.
     */
    fun toJson(obj: Any?): String {
        if (obj == null) return "null"

        // Use Kotlin's runCatching for a clean "zero-fail" flow
        return runCatching {
            mapper.writeValueAsString(obj)
        }.getOrElse { throwable ->
            // FALLBACK: Return a JSON-safe error descriptor
            val className = obj::class.java.name
            val toStringVal = runCatching { obj.toString() }.getOrDefault("[toString() failed]")

            """{"log_error": "Serialization failed", "class": "$className", "toString": "${toStringVal.escapeJson()}", "message": "${throwable.message?.escapeJson()}"}"""
        }
    }

    // Helper to prevent broken JSON in the fallback message
    private fun String.escapeJson() = this.replace("\"", "\\\"").replace("\n", "\\n")
}