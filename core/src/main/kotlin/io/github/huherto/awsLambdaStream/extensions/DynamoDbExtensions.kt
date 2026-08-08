package io.github.huherto.awsLambdaStream.extensions

import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.serialization.Snapshottable

data class DynamoDbExtensions(
    val batchGetRequest: BatchGetItemRequest? = null,
    val batchGetResponse: BatchGetItemResponse? = null,
    val putRequest: PutItemRequest? = null,
    val putResponse: PutItemResponse? = null,
    val queryRequest: QueryRequest? = null,
    val queryResponse: QueryResponse? = null,
    val scanRequest: ScanRequest? = null,
    val updateRequest: UpdateItemRequest? = null,
    val updateResponse: UpdateItemResponse? = null,
) : Snapshottable {
    override fun toSnapshot(): Any {
        return mapOf(
            "batchGetRequest" to batchGetRequest?.toString(),
            "batchGetResponse" to batchGetResponse?.toString(),
            "putRequest" to putRequest?.toString(),
            "putResponse" to putResponse?.toString(),
            "queryRequest" to queryRequest?.toString(),
            "queryResponse" to queryResponse?.toString(),
            "scanRequest" to scanRequest?.toString(),
            "updateRequest" to updateRequest?.toString(),
            "updateResponse" to updateResponse?.toString(),
        ).filterValues { it != null }
    }
}

val UnitOfWork.dynamoDb: DynamoDbExtensions
    get() = getExtension() ?: DynamoDbExtensions()

val UnitOfWork.batchGetRequest: BatchGetItemRequest?
    get() = dynamoDb.batchGetRequest

val UnitOfWork.batchGetResponse: BatchGetItemResponse?
    get() = dynamoDb.batchGetResponse

val UnitOfWork.putRequest: PutItemRequest?
    get() = dynamoDb.putRequest

val UnitOfWork.putResponse: PutItemResponse?
    get() = dynamoDb.putResponse

val UnitOfWork.queryRequest: QueryRequest?
    get() = dynamoDb.queryRequest

val UnitOfWork.queryResponse: QueryResponse?
    get() = dynamoDb.queryResponse

val UnitOfWork.scanRequest: ScanRequest?
    get() = dynamoDb.scanRequest

val UnitOfWork.updateRequest: UpdateItemRequest?
    get() = dynamoDb.updateRequest

val UnitOfWork.updateResponse: UpdateItemResponse?
    get() = dynamoDb.updateResponse

fun UnitOfWork.withBatchGetRequest(request: BatchGetItemRequest?): UnitOfWork =
    withExtension(dynamoDb.copy(batchGetRequest = request))

fun UnitOfWork.withBatchGetResponse(response: BatchGetItemResponse?): UnitOfWork =
    withExtension(dynamoDb.copy(batchGetResponse = response))

fun UnitOfWork.withPutRequest(request: PutItemRequest?): UnitOfWork =
    withExtension(dynamoDb.copy(putRequest = request))

fun UnitOfWork.withPutResponse(response: PutItemResponse?): UnitOfWork =
    withExtension(dynamoDb.copy(putResponse = response))

fun UnitOfWork.withQueryRequest(request: QueryRequest?): UnitOfWork =
    withExtension(dynamoDb.copy(queryRequest = request))

fun UnitOfWork.withQueryResponse(response: QueryResponse?): UnitOfWork =
    withExtension(dynamoDb.copy(queryResponse = response))

fun UnitOfWork.withScanRequest(request: ScanRequest?): UnitOfWork =
    withExtension(dynamoDb.copy(scanRequest = request))

fun UnitOfWork.withUpdateRequest(request: UpdateItemRequest?): UnitOfWork =
    withExtension(dynamoDb.copy(updateRequest = request))

fun UnitOfWork.withUpdateResponse(response: UpdateItemResponse?): UnitOfWork =
    withExtension(dynamoDb.copy(updateResponse = response))
