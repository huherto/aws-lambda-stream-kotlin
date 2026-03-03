package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import kotlinx.serialization.Contextual

interface Event {
    var id: String?
    var type: String
    var timestamp: Long? // In milliseconds since epoch
    var partitionKey: String?
    var tags: Map<String, String>?
    var raw: Any?
    var eem: Any? // Envelope Encryption Metadata. See SAP4SS page 347

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

class FailureEvent() : Event {

    override var id: String? = null
    override var type: String = "FAILURE_EVENT"
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()
    var failureException : FailureException? = null
    
    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null
    override fun encoded(): String {
        TODO("Not yet implemented")
    }

}