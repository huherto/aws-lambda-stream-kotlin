package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.google.gson.GsonBuilder
import com.google.gson.TypeAdapter
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
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

class ByteBufferAdapter : TypeAdapter<ByteBuffer>() {
    override fun write(out: JsonWriter, value: ByteBuffer?) {
        if (value == null) {
            out.nullValue()
        } else {
            // Duplicate the buffer so we don't change the position of the original one
            val duplicate = value.duplicate()
            val bytes = ByteArray(duplicate.remaining())
            duplicate.get(bytes)
            out.value(Base64.getEncoder().encodeToString(bytes))
        }
    }

    override fun read(`in`: JsonReader): ByteBuffer? {
        return null // Deserialization is not needed for logging
    }
}

fun Any?.asJson() : String {
    val gson = GsonBuilder()
        .registerTypeHierarchyAdapter(ByteBuffer::class.java, ByteBufferAdapter())
        .setPrettyPrinting()
        .create()
    return gson.toJson(this)
}