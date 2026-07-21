package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemResponse
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transform
import java.util.concurrent.ConcurrentHashMap

/*
 * These extension functions will be superseded by the new DynamoDbQuery class.
 */

// In-memory cache placeholder (replacing memory-cache)
val memoryCache = ConcurrentHashMap<String, Any>()

/**
 * Common configuration options for DynamoDB operations.
 */
typealias MapDecryptFunc = suspend (Map<String, AttributeValue>) -> Map<String, AttributeValue>

data class DynamoDbOptions(
    // Removed all the options except for decrypt.
    val decrypt: MapDecryptFunc? = null
)

fun Flow<UnitOfWork>.batchGetDynamoDB(
    dynamoDbConnector: DynamoDbConnector,
    options: DynamoDbOptions = DynamoDbOptions()
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.batchGetRequest ?: return@map uow

    val reqKey = request.toString() // Or JSON serialization of request
    var cachedResponse = memoryCache[reqKey] as? BatchGetItemResponse
    if (cachedResponse == null) {
        val result = dynamoDbConnector.batchGetItem(request, uow)
        val decryptedResponses = decryptResponses(result.responses, options.decrypt)
        cachedResponse = result.copy { responses = decryptedResponses }
        memoryCache[reqKey] = cachedResponse
    }
    uow.copy(batchGetResponse = cachedResponse)
}

private suspend fun decryptResponses(
    responses: Map<String, List<Map<String, AttributeValue>>>?,
    decrypt: MapDecryptFunc?
): Map<String, List<Map<String, AttributeValue>>>? {
    if (decrypt == null) return responses
    val decryptedResponses = coroutineScope {
        responses?.mapValues { (_, items) ->
            items.map { item ->
                async { decrypt(item) }
            }.awaitAll()
        }
    }
    return decryptedResponses
}

private suspend fun decryptItems(
    items: List<Map<String, AttributeValue>>,
    decrypt: MapDecryptFunc?
): List<Map<String, AttributeValue>> {
    if (decrypt == null) return items
    return coroutineScope {
            items.map { item ->
                async { decrypt(item) }
            }.awaitAll()
        }
}

fun Flow<UnitOfWork>.queryAllDynamoDB(
    dynamoDbConnector: DynamoDbConnector,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.queryRequest ?: return@map uow

    val reqKey = request.toString()
    var cachedResponse = memoryCache[reqKey] as? QueryResponse
    if (cachedResponse == null) {
        cachedResponse = dynamoDbConnector.queryAll(request, uow)
        memoryCache[reqKey] = cachedResponse
    }
    uow.copy(queryResponse = cachedResponse)
}

fun Flow<UnitOfWork>.scanSplitDynamoDB(
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions()
): Flow<UnitOfWork> = this.transform { uow ->
    val baseRequest = uow.scanRequest ?: run {
        emit(uow)
        return@transform
    }

    var cursor: Map<String, AttributeValue>? = baseRequest.exclusiveStartKey
    var itemsCount = 0

    do {
        val currentRequest = baseRequest.copy {
            exclusiveStartKey = cursor
        }

        val response = dynamoDbClient.scan(currentRequest)

        val decryptedItems = decryptItems(response.items ?: emptyList(), options.decrypt)

        itemsCount += decryptedItems.size

        if (response.lastEvaluatedKey?.isNotEmpty() == true &&
            (currentRequest.limit == null || itemsCount < currentRequest.limit!!)) {
            cursor = response.lastEvaluatedKey
        } else {
            cursor = null
        }

        decryptedItems.forEach { item ->
            // emit(uow.copy(scanRequest = currentRequest, scanResponseItem = item, lastEvaluatedKey = cursor))
            emit(uow)
        }

    } while (cursor != null)
}

fun Flow<UnitOfWork>.querySplitDynamoDB(
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions()
): Flow<UnitOfWork> = this.transform { uow ->
    val baseRequest = uow.queryRequest ?: run {
        emit(uow)
        return@transform
    }

    var cursor: Map<String, AttributeValue>? = baseRequest.exclusiveStartKey
    var itemsCount = 0

    do {
        val currentRequest = baseRequest.copy {
            exclusiveStartKey = cursor
        }

        val response = dynamoDbClient.query(currentRequest)

        val decryptedItems = decryptItems(response.items?: emptyList(), options.decrypt)

        itemsCount += decryptedItems.size

        if (response.lastEvaluatedKey?.isNotEmpty() == true &&
            (currentRequest.limit == null || itemsCount < currentRequest.limit!!)) {
            cursor = response.lastEvaluatedKey
        } else {
            cursor = null
        }

        decryptedItems.forEach { item ->
            // emit(uow.copy(querySplitRequest = currentRequest, querySplitResponseItem = item, lastEvaluatedKey = cursor))
            emit(uow) // Re-emit new sub-uows
        }

    } while (cursor != null)
}

// ------------------------------------------------------------------------
// Request Generators
// ------------------------------------------------------------------------

