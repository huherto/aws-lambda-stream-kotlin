package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.connectors.ConnectorResponse
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore

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

