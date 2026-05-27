package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.s3.model.*
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
    val meta: Map<String, String?>? = null,
    val triggers: List<Event>? = null,
    val correlated: List<Event>? = null,
    val batch: List<UnitOfWork>? = null,

    val batchGetRequest: BatchGetItemRequest? = null,
    val batchGetResponse: BatchGetItemResponse? = null,
    val publishRequest: PutEventsRequest? = null,
    val publishRequestEntry: PutEventsRequestEntry? = null,
    val publishResponse: ConnectorResponse? = null,
    val putRequest: PutItemRequest? = null,
    val putResponse: PutItemResponse? = null,
    val queryParams: EventsMicrostore.QueryParams? = null,
    val queryRequest: QueryRequest? = null,
    val queryResponse: QueryResponse? = null,
    val saveOptions: EventsMicrostore.SaveOptions? = null,
    val scanRequest: ScanRequest? = null,
    val updateRequest: UpdateItemRequest? = null,
    val updateResponse: UpdateItemResponse? = null,

    val s3: S3UnitOfWork = S3UnitOfWork(),
)

data class S3UnitOfWork(
    val getRequest: GetObjectRequest? = null,
    val getResponse: GetObjectResponse? = null,
    val getResponseText: String? = null,
    val getResponseBytes: ByteArray? = null,
    val putRequest: PutObjectRequest? = null,
    val putResponse: PutObjectResponse? = null,
    val listRequest: ListObjectsV2Request? = null,
    val listResponse: ListObjectsV2Response? = null,
    val listResponseObject: Object? = null,
    val headRequest: HeadObjectRequest? = null,
    val headResponse: HeadObjectResponse? = null,
)

fun UnitOfWork.copyS3(
    transform: S3UnitOfWork.() -> S3UnitOfWork,
): UnitOfWork = copy(s3 = s3.transform())

