package io.github.huherto.awsLambdaStream.serialization

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.serialization.json.*

class JacksonSerializationStrategy(
    private val mapper: ObjectMapper = defaultMapper()
) : SerializationStrategy {

    companion object {
        fun defaultMapper(): ObjectMapper = jacksonObjectMapper().apply {
            configure(SerializationFeature.INDENT_OUTPUT, true)
            configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            setSerializationInclusion(JsonInclude.Include.NON_NULL)

            val module = SimpleModule()
            module.addSerializer(JsonElement::class.java, JsonElementSerializer())
            module.addSerializer(JsonObject::class.java, JsonElementSerializer() as JsonSerializer<JsonObject>)
            module.addSerializer(JsonArray::class.java, JsonElementSerializer() as JsonSerializer<JsonArray>)
            module.addSerializer(JsonPrimitive::class.java, JsonElementSerializer() as JsonSerializer<JsonPrimitive>)
            registerModule(module)
        }
    }

    override fun serialize(value: Any?): String {
        if (value == null) return "null"
        return mapper.writeValueAsString(value)
    }

    override fun <T : Any> deserialize(payload: String, targetType: Class<T>): T {
        return mapper.readValue(payload, targetType)
    }
}

class JsonElementSerializer : JsonSerializer<JsonElement>() {
    override fun serialize(value: JsonElement, gen: JsonGenerator, serializers: SerializerProvider) {
        when (value) {
            is JsonPrimitive -> {
                when {
                    value.isString -> gen.writeString(value.content)
                    value.booleanOrNull != null -> gen.writeBoolean(value.boolean)
                    value.intOrNull != null -> gen.writeNumber(value.int)
                    value.longOrNull != null -> gen.writeNumber(value.long)
                    value.doubleOrNull != null -> gen.writeNumber(value.double)
                    value.floatOrNull != null -> gen.writeNumber(value.float)
                    else -> gen.writeNull()
                }
            }
            is JsonObject -> {
                gen.writeStartObject()
                value.forEach { (key, element) ->
                    gen.writeFieldName(key)
                    serialize(element, gen, serializers)
                }
                gen.writeEndObject()
            }
            is JsonArray -> {
                gen.writeStartArray()
                value.forEach { element ->
                    serialize(element, gen, serializers)
                }
                gen.writeEndArray()
            }
            else -> gen.writeNull()
        }
    }
}
