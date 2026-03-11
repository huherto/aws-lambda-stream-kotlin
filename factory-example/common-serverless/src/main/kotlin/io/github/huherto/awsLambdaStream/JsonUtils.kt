package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.ByteBuffer
import java.util.Base64

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

    val module = SimpleModule()
    module.addSerializer(ByteBuffer::class.java, ByteBufferSerializer())
    module.addSerializer(AttributeValue::class.java, AttributeValueSerializer())
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