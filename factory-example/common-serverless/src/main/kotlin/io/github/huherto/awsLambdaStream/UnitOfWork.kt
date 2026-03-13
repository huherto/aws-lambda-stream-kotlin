package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import kotlinx.serialization.Contextual
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.myorg.sut.sutJson

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
    val meta: Map<String, String>? = null
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

class JsonEvent(val jsonString: String) : Event {

    private val logger = mu.KotlinLogging.logger {  }

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
        get() = jsonObject
        set(value) {}
    override var eem: Any?
        get() = TODO("Not yet implemented")
        set(value) {}

    override fun eventType(): String {
        jsonObject["type"]?.jsonPrimitive?.content?.let { return it }
         return "unknown"
    }

    override fun encoded(): String {
        return jsonObject.asJson()
    }


}