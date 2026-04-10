package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.connectors.ConnectorResponse
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore

interface Event {
    var id: String?
    var timestamp: Long? // In milliseconds since epoch
    var partitionKey: String?
    var tags: Map<String, String>?
    var raw: Any?

    // Envelope Encryption Metadata. See SAP4SS page 347
    var eem: Any?

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
    val saveOptions: EventsMicrostore.SaveOptions? = null,
    val queryParams: EventsMicrostore.QueryParams? = null,
    val putRequest: PutItemRequest? = null,
    val putResponse: PutItemResponse? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<Event>? = null,
    val correlated: List<Event>? = null,
    val batch: List<UnitOfWork>? = null,
    val queryResponse: QueryResponse? = null,
    val batchGetRequest: BatchGetItemRequest? = null,
    val queryRequest: QueryRequest? = null,
    val scanRequest: ScanRequest? = null,
    val publishRequestEntry: PutEventsRequestEntry? = null,
    val publishRequest: PutEventsRequest? = null,
    val publishResponse: ConnectorResponse? = null,
)

class FaultException(
    val uow: UnitOfWork,
    cause: Throwable?
) : RuntimeException( cause)

const val FAULT_EVENT_TYPE : String = "fault"

class FaultEvent() : Event {

    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = mutableMapOf()
    var failureException : FaultException? = null
    override var raw: Any? = null
    override var eem: Any? = null
    override fun eventType(): String {
        return FAULT_EVENT_TYPE
    }

    override fun toString(): String {
        return failureException?.cause?.message ?: "Unknown failure"
    }

    override fun encoded(): String {
        return this.asJson()
    }
}
