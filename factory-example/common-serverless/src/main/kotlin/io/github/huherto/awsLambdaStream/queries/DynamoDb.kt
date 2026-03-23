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
data class DynamoDbOptions(
    val pipelineId: String? = null,
    val tableName: String = System.getenv("EVENT_TABLE_NAME") ?: System.getenv("ENTITY_TABLE_NAME") ?: "Table",
    val parallel: Int = System.getenv("PARALLEL")?.toIntOrNull() ?: 4,
    val timeout: Long = System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull() ?: System.getenv("TIMEOUT")?.toLongOrNull() ?: 1000L,
    val step: String = "get",
    val decrypt: suspend (Map<String, AttributeValue>) -> Map<String, AttributeValue> = { it }
)

// Extension properties to access requests/responses dynamically if they are added to `UnitOfWork` or `meta`.
// Note: Depending on your exact UnitOfWork implementation, you might want to adapt these
// to be standard properties of your UoW or store them in the `meta` map.
var UnitOfWork.batchGetRequest: BatchGetItemRequest?
    get() = meta?.get("batchGetRequest") as? BatchGetItemRequest
    set(value) { /* Handle setting based on your UoW structure */ }

var UnitOfWork.queryRequest: QueryRequest?
    get() = toQueryRequest
    set(value) { /* UoW might have to be copied */ }

var UnitOfWork.scanRequest: ScanRequest?
    get() = meta?.get("scanRequest") as? ScanRequest
    set(value) { /* Handle setting */ }


fun Flow<UnitOfWork>.batchGetDynamoDB(
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions(step = "get")
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.batchGetRequest ?: return@map uow

    val reqKey = request.toString() // Or JSON serialization of request
    val cachedResponse = memoryCache[reqKey] as? BatchGetItemResponse

    val response = if (cachedResponse != null) {
        cachedResponse
    } else {
        val result = dynamoDbClient.batchGetItem(request)

        // Decrypt responses
        val decryptedResponses = coroutineScope {
            result.responses?.mapValues { (_, items) ->
                items.map { item ->
                    async { options.decrypt(item) }
                }.awaitAll()
            }
        }

        val finalResponse = result.copy {
            responses = decryptedResponses
        }

        memoryCache[reqKey] = finalResponse
        finalResponse
    }

    // uow.copy(batchGetResponse = response) // Adapt based on actual UoW class structure
    uow
} // Optional: You can implement parallelism using flatMapMerge { flow { emit(invoke(it)) } }


fun Flow<UnitOfWork>.queryAllDynamoDB(
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions(step = "query")
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.queryRequest ?: return@map uow

    val reqKey = request.toString()
    @Suppress("UNCHECKED_CAST")
    val cachedResponse = memoryCache[reqKey] as? List<Map<String, AttributeValue>>

    val response = if (cachedResponse != null) {
        cachedResponse
    } else {
        val result = dynamoDbClient.query(request)

        val decryptedItems = coroutineScope {
            result.items?.map { item ->
                async { options.decrypt(item) }
            }?.awaitAll() ?: emptyList()
        }

        memoryCache[reqKey] = decryptedItems
        decryptedItems
    }

    uow.copy(queryResponse = response)
}

fun Flow<UnitOfWork>.scanSplitDynamoDB(
    dynamoDbClient: DynamoDbClient,
    options: DynamoDbOptions = DynamoDbOptions(step = "scan")
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

        val decryptedItems = coroutineScope {
            response.items?.map { async { options.decrypt(it) } }?.awaitAll() ?: emptyList()
        }

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
    options: DynamoDbOptions = DynamoDbOptions(step = "query")
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

        val decryptedItems = coroutineScope {
            response.items?.map { async { options.decrypt(it) } }?.awaitAll() ?: emptyList()
        }

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
        val value = data[fk] as? String
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