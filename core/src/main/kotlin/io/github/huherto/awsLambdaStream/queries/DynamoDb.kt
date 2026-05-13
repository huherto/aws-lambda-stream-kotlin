package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transform
import java.util.concurrent.ConcurrentHashMap

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
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions()
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.batchGetRequest ?: return@map uow

    val reqKey = request.toString() // Or JSON serialization of request
    var cachedResponse = memoryCache[reqKey] as? BatchGetItemResponse
    if (cachedResponse == null) {
        val result = dynamoDbClient.batchGetItem(request)
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
    dynamoDbClient: DynamoDbClient,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.queryRequest ?: return@map uow

    val reqKey = request.toString()
    var cachedResponse = memoryCache[reqKey] as? QueryResponse
    if (cachedResponse == null) {
        cachedResponse = dynamoDbClient.query(request)
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

        decryptedItems?.forEach { item ->
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

data class Rule(
    val pkFn: String? = null,
    val indexNm: String? = null,
    val indexFn: String? = null,
    val fks: List<String> = emptyList(),
    val tableName: String = ""
)

fun toPkQueryRequest(uow: UnitOfWork, rule: Rule): QueryRequest {
    return QueryRequest {
        keyConditionExpression = "#pk = :pk"
        expressionAttributeNames = mapOf("#pk" to (rule.pkFn ?: "pk"))
        // Assuming your event class has partitionKey property or you extract it safely
        expressionAttributeValues = mapOf(":pk" to AttributeValue.S(uow.event?.partitionKey ?: ""))
        consistentRead = true
    }
}

fun toIndexQueryRequest(uow: UnitOfWork, rule: Rule): QueryRequest {
    return QueryRequest {
        indexName = rule.indexNm
        keyConditionExpression = "#pk = :pk"
        expressionAttributeNames = mapOf("#pk" to (rule.indexFn ?: "pk"))
        expressionAttributeValues = mapOf(":pk" to AttributeValue.S(uow.event?.partitionKey ?: ""))
        consistentRead = false
    }
}

fun toGetRequest(uow: UnitOfWork, rule: Rule): BatchGetItemRequest {
    val raw = uow.event?.raw as? RecordPair
    val rawNew = raw?.new
    val rawOld = raw?.old

    val data = rawNew ?: rawOld ?: return BatchGetItemRequest { }

    val keysList = rule.fks.mapNotNull { fk ->
        val value = data[fk]?.s
        if (value != null) {
            val parts = value.split("|")
            if (parts.size == 2) {
                mapOf(
                    "sk" to AttributeValue.S(parts[0]), // discriminator
                    "pk" to AttributeValue.S(parts[1])
                )
            } else null
        } else {
            null
        }
    }

    return BatchGetItemRequest {
        requestItems = mapOf(
            rule.tableName to KeysAndAttributes {
                keys = keysList
            }
        )
    }
}