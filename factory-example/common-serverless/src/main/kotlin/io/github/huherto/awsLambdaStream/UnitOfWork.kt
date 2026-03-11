package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.serialization.Contextual
import java.nio.ByteBuffer
import java.util.Base64

interface Event {
    var id: String?
    var timestamp: Long? // In milliseconds since epoch
    var partitionKey: String?
    var tags: Map<String, String>?
    var raw: Any?
    var eem: Any?
    // Envelope Encryption Metadata. See SAP4SS page 347

    fun eventType(): String

    fun encoded()  : String
}

data class UnitOfWork(
    val pipeline: Pipeline? = null,
    val record: Any? = null,
    val event: Event? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val putRequest: PutItemRequest? = null,
    val putResponse: PutItemResponse? = null,
)

class FailureException(
    val uom: UnitOfWork,
    cause: Throwable?
) : RuntimeException( cause)

val FAULT_EVENT_TYPE : String = "fault"

class FailureEvent() : Event {

    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()
    var failureException : FailureException? = null
    
    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null
    override fun eventType(): String {
        return FAULT_EVENT_TYPE
    }

    override fun encoded(): String {
        TODO("Not yet implemented")
    }

}

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

private val jacksonMapper = jacksonObjectMapper().apply {
    enable(SerializationFeature.INDENT_OUTPUT) // Pretty print
    disable(SerializationFeature.FAIL_ON_EMPTY_BEANS) // Prevent crashes on objects with no public properties

    val module = SimpleModule()
    module.addSerializer(ByteBuffer::class.java, ByteBufferSerializer())
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
